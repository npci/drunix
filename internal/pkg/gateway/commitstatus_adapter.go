/*
Copyright National Payments Corporation of India. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package gateway

import (
	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/hyperledger/fabric-protos-go/gateway"
	"github.com/npci/drunix/core/peer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

/*
DRUNIX:
newCommittingPeerClient creates and returns a new gRPC GatewayClient
configured to communicate with the committing peer. This client is used
to forward CommitStatus requests and interact with the peer that maintains
the ledger state
*/
func newCommittingPeerClient(config *peer.CommittingPeerConfig) gateway.GatewayClient {

	if !config.ForwardCommitStatusEnabled {
		return nil
	}

	certificatePEM, err := os.ReadFile(config.TlsCertPath)
	if err != nil {
		logger.Errorf("failed to read TLS certificate file [%s]: %v", config.TlsCertPath, err)
		return nil
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(certificatePEM) {
		logger.Errorf("failed to append CA certificate")
		return nil
	}

	tlsConfig := &tls.Config{
		RootCAs:    certPool,
		MinVersion: tls.VersionTLS12,
	}

	transportCredentials := credentials.NewTLS(tlsConfig)

	connection, err := grpc.NewClient(config.PeerEndpoint, grpc.WithTransportCredentials(transportCredentials))
	if err != nil {
		logger.Errorf("failed to create gRPC connection: %w", err)
		return nil

	}

	return gateway.NewGatewayClient(connection)
}
