/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/
package vscc_service

import (
	"github.com/npci/drunix/common/flogging"
	"github.com/npci/drunix/core/config"
	"github.com/spf13/viper"

	"github.com/hyperledger/fabric-protos-go/common"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

var logger = flogging.MustGetLogger("vscc_service")

type Config struct {
	Enabled         bool
	endpoint        string
	tlsEnabled      bool
	tlsCertFile     string
	ingressEndpoint string
	batchSize       int
	grpcsvcconfig   string
}

func LoadConfig() *Config {

	batchSize := viper.GetInt("peer.vsccservice.batchsize")
	if batchSize == 0 {
		batchSize = 10
	}

	return &Config{
		Enabled:         viper.GetBool("peer.vsccservice.enabled"),
		endpoint:        viper.GetString("peer.vsccservice.endpoint"),
		batchSize:       batchSize,
		tlsEnabled:      viper.GetBool("peer.vsccservice.tls.enabled"),
		tlsCertFile:     config.GetPath("peer.tls.cert.file"),
		ingressEndpoint: viper.GetString("peer.vsccservice.ingressendpoint"),
		grpcsvcconfig:   viper.GetString("peer.vsccservice.grpcsvcconfig"),
	}
}

type VsccServiceClient struct {
	Client    common.VsccServiceClient
	BatchSize int
}

func NewVsccServiceClient(vsccConfig *Config) (*VsccServiceClient, error) {

	logger.Infof("vsccConfig: %+v\n", vsccConfig)

	var err error
	var creds = insecure.NewCredentials()
	if vsccConfig.tlsEnabled {
		creds, err = credentials.NewClientTLSFromFile(vsccConfig.tlsCertFile, vsccConfig.ingressEndpoint)
		if err != nil {
			return nil, err
		}
	}

	lbpolicy := vsccConfig.grpcsvcconfig
	if lbpolicy == "" {
		lbpolicy = "round_robin"
	}

	conn, err := grpc.Dial(vsccConfig.endpoint, grpc.WithTransportCredentials(creds), grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"`+lbpolicy+`"}`))

	if err != nil {
		return nil, err
	}

	client := common.NewVsccServiceClient(conn)

	return &VsccServiceClient{Client: client, BatchSize: vsccConfig.batchSize}, nil
}

/*
Test for the load balace in vscc with the grpc connection closing and reopening

//func NewVsccServiceClient(vsccConfig *Config) (*VsccServiceClient, error) {
//	//var creds credentials.TransportCredentials
//	var err error

//	var creds = insecure.NewCredentials()
//	if vsccConfig.tlsEnabled {
//		creds, err = credentials.NewClientTLSFromFile(vsccConfig.tlsCertFile, vsccConfig.ingressEndpoint)
//		if err != nil {
//			return nil, err
//		}
//	}



//	conn, err := grpc.Dial(vsccConfig.endpoint,
//		grpc.WithTransportCredentials(creds),
//		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
//	)
//	if err != nil {
//		return nil, err
//	}

//	wrapper := &VsccServiceClient{BatchSize: vsccConfig.batchSize}
//	wrapper.Client.Store(common.NewVsccServiceClient(conn))

//	// Refresh logic in background
//	go func() {
//		ticker := time.NewTicker(30 * time.Second)
//		for range ticker.C {

//			newConn, err := grpc.Dial(vsccConfig.endpoint,
//				grpc.WithTransportCredentials(creds),
//				grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
//			)
//			if err != nil {
//				fmt.Printf("Failed to refresh gRPC client: %v\n", err)
//				continue
//			}
//			conn.Close()
//			wrapper.Client.Store(common.NewVsccServiceClient(newConn))

//		}
//	}()

//	return wrapper, nil
//}

*/
