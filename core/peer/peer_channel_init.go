/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/



package peer

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/hyperledger/fabric-protos-go/gateway"
	"github.com/npci/drunix/core/committer/txvalidator/v20/plugindispatcher"
	"github.com/npci/drunix/core/ledger"
	"github.com/npci/drunix/internal/pkg/comm"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

/*
DRUNIX
For lite peers, a gRPC stream is established with the committing peer during channel creation
to listen for configuration updates. When a config update event occurs, the lite peer channel is re-initialized.
*/
func (p *Peer) createChannelOnConfigUpdate(
	cid string,
	l ledger.PeerLedger,
	deployedCCInfoProvider ledger.DeployedChaincodeInfoProvider,
	legacyLifecycleValidation plugindispatcher.LifecycleResources,
	newLifecycleValidation plugindispatcher.CollectionAndLifecycleResources,
) error {

	isDone := make(chan error, 1)
	defer close(isDone)

	go p.configEventListener(cid, l, deployedCCInfoProvider, legacyLifecycleValidation, newLifecycleValidation, isDone)

	return <-isDone
}

func newCommittingPeerClient(config *CommittingPeerConfig) (gateway.GatewayClient, error) {

	certificatePEM, err := os.ReadFile(config.TlsCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read TLS certificate file [%s]: %w", config.TlsCertPath, err)
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(certificatePEM) {
		return nil, fmt.Errorf("failed to append CA certificate")
	}

	tlsConfig := &tls.Config{
		RootCAs:    certPool,
		MinVersion: tls.VersionTLS12,
	}

	transportCredentials := credentials.NewTLS(tlsConfig)

	/*Drunix: Apply keepalive parameters so the long-lived ConfigEvents stream is not
	silently dropped by intermediaries (NAT/firewall/LB) during idle periods
	between config updates. Uses comm.DefaultKeepaliveOptions for consistency
	with other Fabric gRPC clients.
	*/
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(transportCredentials),
	}
	dialOpts = append(dialOpts, comm.DefaultKeepaliveOptions.ClientKeepaliveOptions()...)

	connection, err := grpc.NewClient(config.PeerEndpoint, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection: %w", err)
	}

	return gateway.NewGatewayClient(connection), nil
}

func (p *Peer) configEventListener(
	cid string,
	l ledger.PeerLedger,
	deployedCCInfoProvider ledger.DeployedChaincodeInfoProvider,
	legacyLifecycleValidation plugindispatcher.LifecycleResources,
	newLifecycleValidation plugindispatcher.CollectionAndLifecycleResources,
	isDone chan<- error,
) {

	failureCounter := 0
	backoffExponentBase := 2.0
	maxFailures := int(math.Ceil(math.Log(float64(p.CommittingPeerConfig.MaxRetryDelay)/float64(p.CommittingPeerConfig.InitialRetryDelay)) / math.Log(backoffExponentBase)))
	isInitialized := false

	for {

		if failureCounter > 0 {
			var sleepDuration time.Duration
			if failureCounter > maxFailures {
				sleepDuration = p.CommittingPeerConfig.MaxRetryDelay
			} else {
				sleepDuration = time.Duration(float64(p.CommittingPeerConfig.InitialRetryDelay) * math.Pow(backoffExponentBase, float64(failureCounter-1)))
			}
			peerLogger.Errorf("Disconnected from committing peer. Attempt to re-connect in %v. Failure count: %v\n", sleepDuration, failureCounter)
			time.Sleep(sleepDuration)
		}

		client, err := newCommittingPeerClient(p.CommittingPeerConfig)
		if err != nil {
			peerLogger.Errorf("Error ConfigEventListener: %v", err)
			failureCounter++
			continue
		}

		stream, err := client.ConfigEvents(context.Background(), &gateway.ConfigEventsRequest{
			PeerId:    p.PeerId,
			ChannelId: cid,
		})

		if err != nil {
			peerLogger.Errorf("Error ConfigEventListener: %v", err)
			failureCounter++
			continue
		}

		peerLogger.Infof("Connected to committing peer, PEER_ID: %s, PEER_ENDPOINT: %s", p.CommittingPeerConfig.PeerId, p.CommittingPeerConfig.PeerEndpoint)

		failureCounter = 0

		peerLogger.Infof("Loading chain %s", cid)
		// p.LedgerMgr.Close()
		l, err := p.LedgerMgr.OpenLedger(cid)
		if err != nil {
			peerLogger.Errorf("Failed to load ledger %s(%+v)", cid, err)
			peerLogger.Debugf("Error while loading ledger %s with message %s. We continue to the next ledger rather than abort.", cid, err)
			failureCounter++
			continue
		}
		if err := p.createChannel(cid, l, deployedCCInfoProvider, legacyLifecycleValidation, newLifecycleValidation); err != nil {
			peerLogger.Errorf("error creating channel for %s", cid)
			if !isInitialized {
				isInitialized = true
				isDone <- err
				return
			}
			failureCounter++
			continue
		}
		p.initChannel(cid)

		if !isInitialized {
			isInitialized = true
			isDone <- nil
		}

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				peerLogger.Errorf("Error ConfigEventListener: %v", err)
				failureCounter++
				break
			}
			if err != nil {
				peerLogger.Errorf("Error ConfigEventListener: %v", err)
				failureCounter++
				break
			}

			peerLogger.Infof("Received config event : %+v", resp)
			peerLogger.Infof("Loading chain %s", cid)
			// p.LedgerMgr.Close()
			l, err := p.LedgerMgr.OpenLedger(cid)
			if err != nil {
				peerLogger.Errorf("Failed to load ledger %s(%+v)", cid, err)
				peerLogger.Debugf("Error while loading ledger %s with message %s. We continue to the next ledger rather than abort.", cid, err)
				failureCounter++
				break
			}
			if err := p.createChannel(cid, l, deployedCCInfoProvider, legacyLifecycleValidation, newLifecycleValidation); err != nil {
				peerLogger.Errorf("error creating channel for %s", cid)
				failureCounter++
				break
			}
			p.initChannel(cid)
		}
	}
}
