/*
Copyright National Payments Corporation of India. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package publisher

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/hyperledger/fabric-protos-go/gateway"
	"github.com/npci/drunix/common/flogging"
	"github.com/spf13/viper"
)

var logger = flogging.MustGetLogger("chaincode.event.publisher")

const (
	UNIMPLEMENTED ServerType = iota
	NATS
	KAFKA
	GRPC
	AMQP
)

/*
ServerType:
Type: Int

ServerType is an enum for different event server types
*/
type ServerType int

/*
Config:
Type: Struct

Config is the configuration for the event server
Members:
  - ServerType: ServerType (enum) for the event server
  - Hosts: list of hosts for the event server
  - MspId: MSP ID of the client
  - ClientCertificate: path to the client certificate
  - ClientPrivateKey: path to the client private key
  - CACertificates: list of paths to CA certificates
  - AMQP: settings used only by the AMQP 1.0 event server
*/
type Config struct {
	// OrgType           chaincode.OrgType
	ServerType        ServerType
	Hosts             []string
	MspId             string
	ClientCertificate string
	ClientPrivateKey  string
	CACertificates    []string
	Topics            []string
	PeerEndpoint      string
	ShardEnabled      bool
	WorkerPoolSize    int
	AMQP              AMQPConfig
}

/*
AMQPConfig:
Type: Struct

AMQPConfig is the configuration specific to the AMQP 1.0 event server
Members:
  - NetworkID: logical network name, used as a level of the event address
  - AddressPrefix: prefix prepended to every event address
  - TopicSeparator: separator between the levels of the event address
  - SASLMechanism: SASL mechanism used to authenticate (anonymous, plain or external)
  - Username: username for the plain SASL mechanism
  - Password: password for the plain SASL mechanism
  - OnUndeliverable: action when the server does not accept an event (block or skip)
*/
type AMQPConfig struct {
	NetworkID       string
	AddressPrefix   string
	TopicSeparator  string
	SASLMechanism   string
	Username        string
	Password        string
	OnUndeliverable string
}

/*
String:
Type: Method

String returns the AMQP configuration with the password masked, so that it can be logged safely.
*/
func (c AMQPConfig) String() string {
	password := ""
	if c.Password != "" {
		password = "****"
	}
	return fmt.Sprintf("{NetworkID:%s AddressPrefix:%s TopicSeparator:%s SASLMechanism:%s Username:%s Password:%s OnUndeliverable:%s}",
		c.NetworkID, c.AddressPrefix, c.TopicSeparator, c.SASLMechanism, c.Username, password, c.OnUndeliverable)
}

/*
GoString:
Type: Method

GoString returns the masked AMQP configuration for the %#v format verb.
*/
func (c AMQPConfig) GoString() string {
	return c.String()
}

func LoadConfig() Config {
	var config Config

	serverType := viper.GetString("peer.chaincode.eventpublisher.serverType")
	// serverType := "kafka"
	switch strings.ToLower(serverType) {
	case "nats":
		config.ServerType = NATS
	case "kafka":
		config.ServerType = KAFKA
	case "grpc":
		config.ServerType = GRPC
	case "amqp":
		config.ServerType = AMQP
	default:
		config.ServerType = UNIMPLEMENTED
		// skip reading other config if serverType is not set
		return config
	}

	config.Hosts = viper.GetStringSlice("peer.chaincode.eventpublisher.hosts")
	config.MspId = viper.GetString("peer.localMspId")
	config.ClientCertificate = viper.GetString("peer.chaincode.eventpublisher.clientCertificate")
	config.ClientPrivateKey = viper.GetString("peer.chaincode.eventpublisher.clientPrivateKey")
	config.CACertificates = viper.GetStringSlice("peer.chaincode.eventpublisher.caCertificates")
	config.Topics = viper.GetStringSlice("peer.chaincode.eventpublisher.topics")
	// config.OrgType = chaincode.OrgType(viper.GetString("peer.chaincode.eventpublisher.orgType"))
	config.PeerEndpoint = viper.GetString("peer.address")

	config.WorkerPoolSize = viper.GetInt("peer.chaincode.eventpublisher.workerpoolsize")
	if config.WorkerPoolSize == 0 {
		config.WorkerPoolSize = runtime.NumCPU() * 4
	}

	if config.ServerType == AMQP {
		config.AMQP = AMQPConfig{
			NetworkID:       viper.GetString("peer.networkId"),
			AddressPrefix:   viper.GetString("peer.chaincode.eventpublisher.amqp.addressPrefix"),
			TopicSeparator:  viper.GetString("peer.chaincode.eventpublisher.amqp.topicSeparator"),
			SASLMechanism:   viper.GetString("peer.chaincode.eventpublisher.amqp.saslMechanism"),
			Username:        viper.GetString("peer.chaincode.eventpublisher.amqp.username"),
			Password:        viper.GetString("peer.chaincode.eventpublisher.amqp.password"),
			OnUndeliverable: viper.GetString("peer.chaincode.eventpublisher.amqp.onUndeliverable"),
		}
	}

	// config.Hosts = []string{"192.168.0.105:9092"}
	// config.Topics = []string{"IndentTopic", "IssuanceTopic", "RedeemTopic", "TransferTopic", "WalletTopic", "ConfigTopic"}
	// config.OrgType = "bank"
	// config.ShardEnabled = true

	logger.Infof("Event publisher config: %+v", config)

	return config
}

/*
Publisher:
Type: Interface

Publisher provides functionalities to publish chaincode events
Methods:
	- Publish: publishes the chaincode events
*/
//go:generate mockery --exported --dir ./ --name Publisher --case underscore --output mocks
type Publisher interface {
	// Publish sends events to chaincode events hub
	Publish(channel string, events *gateway.ChaincodeEventsResponse) error
}

/*
NewPublisher:
Type: Struct

UnimplementedPublisher is a publisher that does nothing when Publish is called.
*/
type UnimplementedPublisher struct{}

func NewUnimplementedPublisher() *UnimplementedPublisher {
	logger.Info("created unimplemented event publisher which does nothing when publish is called")
	return &UnimplementedPublisher{}
}

/*
Publish:
Type: Function

Publish does nothing and returns nil.
*/
func (u *UnimplementedPublisher) Publish(_ string, _ *gateway.ChaincodeEventsResponse) error {
	logger.Warnf("Publish called on unimplemented publisher")
	return nil
}
