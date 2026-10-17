/*
Copyright National Payments Corporation of India. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package amqp

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	goamqp "github.com/Azure/go-amqp"
	"github.com/hyperledger/fabric-protos-go/gateway"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/npci/drunix/internal/pkg/gateway/event/publisher"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	t.Cleanup(viper.Reset)
	viper.Set("peer.networkId", "dev")
	viper.Set("peer.localMspId", "Org1MSP")
	viper.Set("peer.chaincode.eventpublisher.serverType", "AMQP")
	viper.Set("peer.chaincode.eventpublisher.hosts", []string{"amqp://broker:5672"})
	viper.Set("peer.chaincode.eventpublisher.amqp.addressPrefix", "topic://")
	viper.Set("peer.chaincode.eventpublisher.amqp.topicSeparator", ".")
	viper.Set("peer.chaincode.eventpublisher.amqp.saslMechanism", "plain")
	viper.Set("peer.chaincode.eventpublisher.amqp.username", "user")
	viper.Set("peer.chaincode.eventpublisher.amqp.password", "secret-password")
	viper.Set("peer.chaincode.eventpublisher.amqp.onUndeliverable", "skip")

	config := publisher.LoadConfig()

	require.Equal(t, publisher.AMQP, config.ServerType)
	require.Equal(t, []string{"amqp://broker:5672"}, config.Hosts)
	require.Equal(t, "Org1MSP", config.MspId)
	require.Equal(t, publisher.AMQPConfig{
		NetworkID:       "dev",
		AddressPrefix:   "topic://",
		TopicSeparator:  ".",
		SASLMechanism:   "plain",
		Username:        "user",
		Password:        "secret-password",
		OnUndeliverable: "skip",
	}, config.AMQP)

	for _, format := range []string{"%v", "%+v", "%#v", "%s"} {
		require.NotContains(t, fmt.Sprintf(format, config), "secret-password", format)
		require.NotContains(t, fmt.Sprintf(format, config.AMQP), "secret-password", format)
	}
	require.Contains(t, fmt.Sprintf("%+v", config), "Password:****")
}

func TestServerTypeValues(t *testing.T) {
	// existing values must not be renumbered
	require.Equal(t, publisher.ServerType(0), publisher.UNIMPLEMENTED)
	require.Equal(t, publisher.ServerType(1), publisher.NATS)
	require.Equal(t, publisher.ServerType(2), publisher.KAFKA)
	require.Equal(t, publisher.ServerType(3), publisher.GRPC)
	require.Equal(t, publisher.ServerType(4), publisher.AMQP)
}

func TestAddress(t *testing.T) {
	tests := []struct {
		name      string
		prefix    string
		separator string
		network   string
		channel   string
		chaincode string
		event     string
		want      string
	}{
		{
			name:      "slash separator without prefix",
			separator: "/",
			network:   "dev",
			channel:   "mychannel",
			chaincode: "basic",
			event:     "AssetTransferred",
			want:      "drunix/dev/mychannel/chaincode/basic/AssetTransferred",
		},
		{
			name:      "dot separator with prefix",
			prefix:    "/exchanges/amq.topic/",
			separator: ".",
			network:   "dev",
			channel:   "mychannel",
			chaincode: "basic",
			event:     "AssetTransferred",
			want:      "/exchanges/amq.topic/drunix.dev.mychannel.chaincode.basic.AssetTransferred",
		},
		{
			name:      "scheme prefix",
			prefix:    "topic://",
			separator: "/",
			network:   "prod",
			channel:   "ch1",
			chaincode: "token-cc",
			event:     "Mint_Event",
			want:      "topic://drunix/prod/ch1/chaincode/token-cc/Mint_Event",
		},
		{
			name:      "values cannot add levels or wildcards",
			separator: "/",
			network:   "dev",
			channel:   "my.channel",
			chaincode: "a/b",
			event:     "x.*>#+ y",
			want:      "drunix/dev/my_channel/chaincode/a_b/x______y",
		},
		{
			name:      "dot in channel name with dot separator",
			separator: ".",
			network:   "dev",
			channel:   "my.channel",
			chaincode: "cc",
			event:     "ev",
			want:      "drunix.dev.my_channel.chaincode.cc.ev",
		},
		{
			name:      "empty and unicode values",
			separator: "/",
			network:   "",
			channel:   "ch",
			chaincode: "cc",
			event:     "évènement",
			want:      "drunix/_/ch/chaincode/cc/_v_nement",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &AMQPPublisher{config: publisher.Config{AMQP: publisher.AMQPConfig{
				NetworkID:      tt.network,
				AddressPrefix:  tt.prefix,
				TopicSeparator: tt.separator,
			}}}
			address := p.address(tt.channel, tt.chaincode, tt.event)
			require.Equal(t, tt.want, address)

			levels := strings.Split(strings.TrimPrefix(address, tt.prefix), tt.separator)
			require.Len(t, levels, 6)
		})
	}
}

func TestNewMessage(t *testing.T) {
	t.Setenv("CORE_PEER_ID", "peer0.org1.example.com")
	p := &AMQPPublisher{config: publisher.Config{
		MspId: "Org1MSP",
		AMQP:  withDefaults(publisher.AMQPConfig{NetworkID: "dev", AddressPrefix: "topic://"}),
	}}
	event := &peer.ChaincodeEvent{
		ChaincodeId: "basic",
		TxId:        "tx1",
		EventName:   "AssetCreated",
		Payload:     []byte("payload"),
	}

	msg, err := p.newMessage("mychannel", 7, 2, event)
	require.NoError(t, err)

	// same JSON layout as the Kafka publisher payload
	require.Equal(t, [][]byte{[]byte(`{"BlockNumber":7,"TransactionID":"tx1/peer0.org1.example.com","ChaincodeName":"basic","EventName":"AssetCreated","Payload":"cGF5bG9hZA=="}`)}, msg.Data)

	encoded, err := msg.MarshalBinary()
	require.NoError(t, err)
	decoded := &goamqp.Message{}
	require.NoError(t, decoded.UnmarshalBinary(encoded))

	require.True(t, decoded.Header.Durable)
	require.Equal(t, uint8(4), decoded.Header.Priority)
	require.Equal(t, "mychannel/7/2", decoded.Properties.MessageID)
	require.Equal(t, "topic://drunix/dev/mychannel/chaincode/basic/AssetCreated", *decoded.Properties.To)
	require.Equal(t, "AssetCreated", *decoded.Properties.Subject)
	require.Equal(t, "application/json", *decoded.Properties.ContentType)
	require.Equal(t, "mychannel", *decoded.Properties.GroupID)
	require.NotNil(t, decoded.Properties.CreationTime)
	require.Equal(t, map[string]any{
		"channel":     "mychannel",
		"chaincode":   "basic",
		"event":       "AssetCreated",
		"txId":        "tx1",
		"mspId":       "Org1MSP",
		"blockNumber": int64(7),
		"eventIndex":  int64(2),
	}, decoded.ApplicationProperties)
}

func TestValidateConfig(t *testing.T) {
	certFile, keyFile := writeTestCertificate(t)

	tests := []struct {
		name    string
		config  publisher.Config
		wantErr string
	}{
		{
			name:   "valid anonymous",
			config: publisher.Config{Hosts: []string{"amqp://broker:5672"}},
		},
		{
			name:   "valid plain with tls and prefix",
			config: publisher.Config{Hosts: []string{"amqps://broker:5671"}, CACertificates: []string{certFile}, AMQP: publisher.AMQPConfig{Username: "user", Password: "pw", AddressPrefix: "topic://", TopicSeparator: "."}},
		},
		{
			name:   "valid external",
			config: publisher.Config{Hosts: []string{"amqps://broker"}, ClientCertificate: certFile, ClientPrivateKey: keyFile, AMQP: publisher.AMQPConfig{SASLMechanism: "EXTERNAL"}},
		},
		{
			name:    "no hosts",
			config:  publisher.Config{},
			wantErr: "no broker url passed",
		},
		{
			name:    "unsupported scheme",
			config:  publisher.Config{Hosts: []string{"tcp://broker:5672"}},
			wantErr: "must use the amqp:// or amqps:// scheme",
		},
		{
			name:    "missing scheme",
			config:  publisher.Config{Hosts: []string{"broker:5672"}},
			wantErr: "must use the amqp:// or amqps:// scheme",
		},
		{
			name:    "credentials in url",
			config:  publisher.Config{Hosts: []string{"amqp://user:pw@broker:5672"}},
			wantErr: "contains credentials",
		},
		{
			name:    "tls certificates with plain amqp",
			config:  publisher.Config{Hosts: []string{"amqp://broker:5672"}, CACertificates: []string{certFile}},
			wantErr: "does not use amqps://",
		},
		{
			name:    "invalid separator",
			config:  publisher.Config{Hosts: []string{"amqp://broker"}, AMQP: publisher.AMQPConfig{TopicSeparator: ":"}},
			wantErr: "invalid AMQP topic separator",
		},
		{
			name:    "wildcard in prefix",
			config:  publisher.Config{Hosts: []string{"amqp://broker"}, AMQP: publisher.AMQPConfig{AddressPrefix: "topic://a/*/"}},
			wantErr: "must not contain wildcard characters",
		},
		{
			name:    "plain without username",
			config:  publisher.Config{Hosts: []string{"amqp://broker"}, AMQP: publisher.AMQPConfig{SASLMechanism: "plain"}},
			wantErr: "requires a username",
		},
		{
			name:    "external without client certificate",
			config:  publisher.Config{Hosts: []string{"amqps://broker"}, AMQP: publisher.AMQPConfig{SASLMechanism: "external"}},
			wantErr: "requires a client certificate",
		},
		{
			name:    "unsupported mechanism",
			config:  publisher.Config{Hosts: []string{"amqp://broker"}, AMQP: publisher.AMQPConfig{SASLMechanism: "scram-sha-256"}},
			wantErr: "unsupported AMQP SASL mechanism",
		},
		{
			name:    "invalid onUndeliverable",
			config:  publisher.Config{Hosts: []string{"amqp://broker"}, AMQP: publisher.AMQPConfig{OnUndeliverable: "drop"}},
			wantErr: "invalid AMQP onUndeliverable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := tt.config
			config.AMQP = withDefaults(config.AMQP)
			err := validateConfig(config)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
			require.NotContains(t, err.Error(), "pw@", "credentials must not be logged")
		})
	}
}

func TestWithDefaults(t *testing.T) {
	require.Equal(t, publisher.AMQPConfig{TopicSeparator: "/", SASLMechanism: "anonymous", OnUndeliverable: "block"}, withDefaults(publisher.AMQPConfig{}))
	require.Equal(t, "plain", withDefaults(publisher.AMQPConfig{Username: "user"}).SASLMechanism)
	require.Equal(t, "external", withDefaults(publisher.AMQPConfig{SASLMechanism: "EXTERNAL"}).SASLMechanism)
	require.Equal(t, "skip", withDefaults(publisher.AMQPConfig{OnUndeliverable: "Skip"}).OnUndeliverable)
}

func TestNewConnOptions(t *testing.T) {
	certFile, keyFile := writeTestCertificate(t)

	options, err := newConnOptions(publisher.Config{AMQP: withDefaults(publisher.AMQPConfig{})})
	require.NoError(t, err)
	require.NotNil(t, options.SASLType)
	require.Nil(t, options.TLSConfig)
	require.Equal(t, "drunix", options.Properties["product"])
	require.Contains(t, options.Properties, "version")

	options, err = newConnOptions(publisher.Config{CACertificates: []string{certFile}, ClientCertificate: certFile, ClientPrivateKey: keyFile, AMQP: withDefaults(publisher.AMQPConfig{SASLMechanism: "external"})})
	require.NoError(t, err)
	require.NotNil(t, options.TLSConfig)
	require.NotNil(t, options.TLSConfig.RootCAs)
	require.Len(t, options.TLSConfig.Certificates, 1)

	_, err = newConnOptions(publisher.Config{CACertificates: []string{filepath.Join(t.TempDir(), "missing.pem")}, AMQP: withDefaults(publisher.AMQPConfig{})})
	require.ErrorContains(t, err, "failed to read CA certificate")

	_, err = newConnOptions(publisher.Config{CACertificates: []string{keyFile}, AMQP: withDefaults(publisher.AMQPConfig{})})
	require.ErrorContains(t, err, "no certificates found")
}

func TestContainerID(t *testing.T) {
	t.Setenv("CORE_PEER_ID", "peer0.org1.example.com")
	first := containerID()
	require.True(t, strings.HasPrefix(first, "peer0.org1.example.com-"))
	require.NotEqual(t, first, containerID())
}

func TestNewAMQPPublisher(t *testing.T) {
	tests := []struct {
		name    string
		config  publisher.Config
		wantErr string
	}{
		{
			name:    "no broker url",
			config:  publisher.Config{},
			wantErr: "no broker url passed",
		},
		{
			name:    "broker not available",
			config:  publisher.Config{Hosts: []string{"amqp://127.0.0.1:1"}},
			wantErr: "failed to connect to AMQP server",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewAMQPPublisher(tt.config)
			require.ErrorContains(t, err, tt.wantErr)
			require.Nil(t, got)
		})
	}
}

func TestOutcomeError(t *testing.T) {
	var undeliverable *undeliverableError

	require.NoError(t, outcomeError(&goamqp.StateAccepted{}))

	err := outcomeError(&goamqp.StateRejected{Error: &goamqp.Error{Condition: goamqp.ErrCondNotFound, Description: "no such node"}})
	require.ErrorAs(t, err, &undeliverable)
	require.ErrorContains(t, err, "no such node")

	require.ErrorAs(t, outcomeError(&goamqp.StateRejected{}), &undeliverable)
	require.ErrorAs(t, outcomeError(&goamqp.StateRejected{Error: &goamqp.Error{Condition: goamqp.ErrCondResourceLimitExceeded}}), &undeliverable)
	require.ErrorAs(t, outcomeError(&goamqp.StateReleased{}), &undeliverable)
	require.ErrorAs(t, outcomeError(&goamqp.StateModified{UndeliverableHere: true}), &undeliverable)

	transient := []goamqp.DeliveryState{
		&goamqp.StateModified{DeliveryFailed: true},
		&goamqp.StateReceived{},
		nil,
	}
	for _, state := range transient {
		err := outcomeError(state)
		require.Error(t, err)
		require.False(t, errors.As(err, &undeliverable), "%T must be retried", state)
	}
}

func TestClassifyError(t *testing.T) {
	var undeliverable *undeliverableError

	tooLarge := &goamqp.Error{Condition: goamqp.ErrCondMessageSizeExceeded}
	require.ErrorAs(t, classifyError(tooLarge), &undeliverable)
	require.ErrorAs(t, classifyError(&goamqp.LinkError{RemoteErr: tooLarge}), &undeliverable)

	connErr := &goamqp.ConnError{}
	require.Same(t, connErr, classifyError(connErr))
	require.Equal(t, context.DeadlineExceeded, classifyError(context.DeadlineExceeded))
}

func TestPublish(t *testing.T) {
	rejected := &goamqp.StateRejected{Error: &goamqp.Error{Condition: goamqp.ErrCondNotFound}}
	connectionLost := &goamqp.ConnError{}

	tests := []struct {
		name            string
		events          int
		onUndeliverable string
		connectErrs     int
		respond         func(id string, attempt int) result
		wantAccepted    []string
		wantSent        []string
		wantConnects    int
		wantSleeps      int
	}{
		{
			name:         "empty block",
			events:       0,
			wantConnects: 0,
		},
		{
			name:         "all accepted in block order",
			events:       3,
			wantAccepted: []string{"ch1/5/0", "ch1/5/1", "ch1/5/2"},
			wantSent:     []string{"ch1/5/0", "ch1/5/1", "ch1/5/2"},
			wantConnects: 1,
		},
		{
			name:   "rejected event is retried with block",
			events: 3,
			respond: func(id string, attempt int) result {
				if id == "ch1/5/1" && attempt < 3 {
					return result{state: rejected}
				}
				return result{}
			},
			wantAccepted: []string{"ch1/5/0", "ch1/5/1", "ch1/5/2"},
			wantSent:     []string{"ch1/5/0", "ch1/5/1", "ch1/5/2", "ch1/5/1", "ch1/5/2", "ch1/5/1", "ch1/5/2"},
			wantConnects: 1,
			wantSleeps:   2,
		},
		{
			name:            "rejected event is skipped with skip",
			events:          3,
			onUndeliverable: "skip",
			respond: func(id string, _ int) result {
				if id == "ch1/5/1" {
					return result{state: rejected}
				}
				return result{}
			},
			wantAccepted: []string{"ch1/5/0", "ch1/5/2"},
			wantSent:     []string{"ch1/5/0", "ch1/5/1", "ch1/5/2", "ch1/5/2"},
			wantConnects: 1,
		},
		{
			name:            "released event is skipped with skip",
			events:          2,
			onUndeliverable: "skip",
			respond: func(id string, _ int) result {
				if id == "ch1/5/0" {
					return result{state: &goamqp.StateReleased{}}
				}
				return result{}
			},
			wantAccepted: []string{"ch1/5/1"},
			wantSent:     []string{"ch1/5/0", "ch1/5/1", "ch1/5/1"},
			wantConnects: 1,
		},
		{
			name:   "rejected for a resource limit is retried with block",
			events: 2,
			respond: func(id string, attempt int) result {
				if id == "ch1/5/0" && attempt == 1 {
					return result{state: &goamqp.StateRejected{Error: &goamqp.Error{Condition: goamqp.ErrCondResourceLimitExceeded}}}
				}
				return result{}
			},
			wantAccepted: []string{"ch1/5/0", "ch1/5/1"},
			wantSent:     []string{"ch1/5/0", "ch1/5/1", "ch1/5/0", "ch1/5/1"},
			wantConnects: 1,
			wantSleeps:   1,
		},
		{
			name:            "rejected for a resource limit is skipped with skip",
			events:          2,
			onUndeliverable: "skip",
			respond: func(id string, _ int) result {
				if id == "ch1/5/0" {
					return result{state: &goamqp.StateRejected{Error: &goamqp.Error{Condition: goamqp.ErrCondResourceLimitExceeded}}}
				}
				return result{}
			},
			wantAccepted: []string{"ch1/5/1"},
			wantSent:     []string{"ch1/5/0", "ch1/5/1", "ch1/5/1"},
			wantConnects: 1,
		},
		{
			name:   "modified event is retried",
			events: 1,
			respond: func(_ string, attempt int) result {
				if attempt == 1 {
					return result{state: &goamqp.StateModified{DeliveryFailed: true}}
				}
				return result{}
			},
			wantAccepted: []string{"ch1/5/0"},
			wantSent:     []string{"ch1/5/0", "ch1/5/0"},
			wantConnects: 1,
			wantSleeps:   1,
		},
		{
			name:   "connection lost while waiting reconnects and resends in order",
			events: 3,
			respond: func(id string, attempt int) result {
				if id == "ch1/5/1" && attempt == 1 {
					return result{waitErr: connectionLost}
				}
				return result{}
			},
			wantAccepted: []string{"ch1/5/0", "ch1/5/1", "ch1/5/2"},
			wantSent:     []string{"ch1/5/0", "ch1/5/1", "ch1/5/2", "ch1/5/1", "ch1/5/2"},
			wantConnects: 2,
			wantSleeps:   1,
		},
		{
			name:   "accept timeout reconnects and resends",
			events: 2,
			respond: func(id string, attempt int) result {
				if id == "ch1/5/0" && attempt == 1 {
					return result{waitErr: context.DeadlineExceeded}
				}
				return result{}
			},
			wantAccepted: []string{"ch1/5/0", "ch1/5/1"},
			wantSent:     []string{"ch1/5/0", "ch1/5/1", "ch1/5/0", "ch1/5/1"},
			wantConnects: 2,
			wantSleeps:   1,
		},
		{
			name:   "send error waits for earlier events then resends the rest",
			events: 3,
			respond: func(id string, attempt int) result {
				if id == "ch1/5/2" && attempt == 1 {
					return result{sendErr: connectionLost}
				}
				return result{}
			},
			wantAccepted: []string{"ch1/5/0", "ch1/5/1", "ch1/5/2"},
			wantSent:     []string{"ch1/5/0", "ch1/5/1", "ch1/5/2"},
			wantConnects: 2,
			wantSleeps:   1,
		},
		{
			name:            "message too large is skipped with skip",
			events:          2,
			onUndeliverable: "skip",
			respond: func(id string, _ int) result {
				if id == "ch1/5/0" {
					return result{sendErr: &goamqp.Error{Condition: goamqp.ErrCondMessageSizeExceeded}}
				}
				return result{}
			},
			wantAccepted: []string{"ch1/5/1"},
			wantSent:     []string{"ch1/5/1"},
			wantConnects: 2,
		},
		{
			name:         "server unavailable is retried until it connects",
			events:       1,
			connectErrs:  2,
			wantAccepted: []string{"ch1/5/0"},
			wantSent:     []string{"ch1/5/0"},
			wantConnects: 3,
			wantSleeps:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := &fakeServer{respond: tt.respond, connectErrs: tt.connectErrs}
			p, sleeps := newTestPublisher(server, publisher.AMQPConfig{OnUndeliverable: tt.onUndeliverable})

			err := p.Publish("ch1", newEvents(5, tt.events))
			require.NoError(t, err)

			require.Equal(t, tt.wantAccepted, server.accepted)
			require.Equal(t, tt.wantSent, server.sent)
			require.Equal(t, tt.wantConnects, server.connects)
			require.Len(t, *sleeps, tt.wantSleeps)
		})
	}
}

func TestPublishBackoff(t *testing.T) {
	server := &fakeServer{respond: func(_ string, attempt int) result {
		if attempt <= 8 {
			return result{state: &goamqp.StateReleased{}}
		}
		return result{}
	}}
	p, sleeps := newTestPublisher(server, publisher.AMQPConfig{})

	require.NoError(t, p.Publish("ch1", newEvents(1, 1)))
	require.Equal(t, []time.Duration{
		1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second,
		16 * time.Second, 30 * time.Second, 30 * time.Second, 30 * time.Second,
	}, *sleeps)
}

func TestPublishRecoversFromPanic(t *testing.T) {
	server := &fakeServer{respond: func(string, int) result { panic("boom") }}
	p, _ := newTestPublisher(server, publisher.AMQPConfig{})

	err := p.Publish("ch1", newEvents(1, 1))
	require.ErrorContains(t, err, "panic while publishing events: boom")
}

func TestPublishConcurrentChannels(t *testing.T) {
	server := &fakeServer{respond: func(id string, attempt int) result {
		if strings.HasSuffix(id, "/1") && attempt == 1 {
			return result{waitErr: &goamqp.ConnError{}}
		}
		return result{}
	}}
	p, _ := newTestPublisher(server, publisher.AMQPConfig{})

	var wg sync.WaitGroup
	for _, channel := range []string{"ch1", "ch2", "ch3", "ch4"} {
		wg.Add(1)
		go func(channel string) {
			defer wg.Done()
			for block := uint64(1); block <= 5; block++ {
				assert.NoError(t, p.Publish(channel, newEvents(block, 3)))
			}
		}(channel)
	}
	wg.Wait()

	server.mutex.Lock()
	defer server.mutex.Unlock()
	for _, channel := range []string{"ch1", "ch2", "ch3", "ch4"} {
		var want, got []string
		for block := 1; block <= 5; block++ {
			for index := 0; index < 3; index++ {
				want = append(want, fmt.Sprintf("%s/%d/%d", channel, block, index))
			}
		}
		for _, id := range server.accepted {
			if strings.HasPrefix(id, channel+"/") {
				got = append(got, id)
			}
		}
		require.Equal(t, want, got, "events of %s must be accepted in order", channel)
	}
}

// TestAMQPPublisherBroker publishes events to a running AMQP 1.0 server and reads them back.
// It only runs when DRUNIX_AMQP_TEST_URL is set, for example:
//
//	DRUNIX_AMQP_TEST_URL=amqp://localhost:5672 DRUNIX_AMQP_TEST_USERNAME=artemis DRUNIX_AMQP_TEST_PASSWORD=artemis \
//	DRUNIX_AMQP_TEST_TOPIC_SEPARATOR=. go test -run TestAMQPPublisherBroker ./internal/pkg/gateway/event/amqp/
//
// DRUNIX_AMQP_TEST_ADDRESS_PREFIX and DRUNIX_AMQP_TEST_TOPIC_SEPARATOR configure the address, and
// DRUNIX_AMQP_TEST_SOURCE sets the address to receive from when it differs from the published
// address, such as a queue bound to the published address.
func TestAMQPPublisherBroker(t *testing.T) {
	url := os.Getenv("DRUNIX_AMQP_TEST_URL")
	if url == "" {
		t.Skip("DRUNIX_AMQP_TEST_URL is not set")
	}
	t.Setenv("CORE_PEER_ID", "peer0")

	config := publisher.Config{
		Hosts: []string{url},
		MspId: "Org1MSP",
		AMQP: publisher.AMQPConfig{
			NetworkID:      "test",
			AddressPrefix:  os.Getenv("DRUNIX_AMQP_TEST_ADDRESS_PREFIX"),
			TopicSeparator: os.Getenv("DRUNIX_AMQP_TEST_TOPIC_SEPARATOR"),
			Username:       os.Getenv("DRUNIX_AMQP_TEST_USERNAME"),
			Password:       os.Getenv("DRUNIX_AMQP_TEST_PASSWORD"),
		},
	}

	p, err := NewAMQPPublisher(config)
	require.NoError(t, err)
	t.Cleanup(func() {
		if l, err := p.currentLink(); err == nil {
			p.resetLink(l)
		}
	})

	channel := fmt.Sprintf("ch%d", time.Now().UnixNano())
	source := os.Getenv("DRUNIX_AMQP_TEST_SOURCE")
	if source == "" {
		source = p.address(channel, "testcc", "TestEvent")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	options, err := newConnOptions(p.config)
	require.NoError(t, err)
	conn, err := goamqp.Dial(ctx, url, options)
	require.NoError(t, err)
	defer conn.Close()
	session, err := conn.NewSession(ctx, nil)
	require.NoError(t, err)
	receiver, err := session.NewReceiver(ctx, source, nil)
	require.NoError(t, err)

	const blocks, eventsPerBlock = 3, 4
	for block := uint64(1); block <= blocks; block++ {
		events := newEvents(block, eventsPerBlock)
		for _, event := range events.Events {
			event.ChaincodeId = "testcc"
			event.EventName = "TestEvent"
		}
		require.NoError(t, p.Publish(channel, events))
	}

	for block := 1; block <= blocks; block++ {
		for index := 0; index < eventsPerBlock; index++ {
			msg, err := receiver.Receive(ctx, nil)
			require.NoError(t, err)
			require.NoError(t, receiver.AcceptMessage(ctx, msg))

			require.Equal(t, fmt.Sprintf("%s/%d/%d", channel, block, index), msg.Properties.MessageID)
			require.Equal(t, int64(block), msg.ApplicationProperties["blockNumber"])
			require.Equal(t, int64(index), msg.ApplicationProperties["eventIndex"])
			require.Equal(t, "testcc", msg.ApplicationProperties["chaincode"])
			require.Equal(t, fmt.Sprintf(`{"BlockNumber":%d,"TransactionID":"tx-%d-%d/peer0","ChaincodeName":"testcc","EventName":"TestEvent","Payload":"cGF5bG9hZA=="}`, block, block, index), string(msg.GetData()))
		}
	}
}

type result struct {
	sendErr error
	waitErr error
	state   goamqp.DeliveryState
}

// fakeServer records the messages sent over the links it creates, and settles them as scripted by
// respond, which receives the message id and the number of times the message has been sent.
type fakeServer struct {
	mutex       sync.Mutex
	respond     func(id string, attempt int) result
	connectErrs int
	connects    int
	attempts    map[string]int
	sent        []string
	accepted    []string
}

func (s *fakeServer) connect(context.Context) (link, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.connects++
	if s.connects <= s.connectErrs {
		return nil, errors.New("connection refused")
	}
	return &fakeLink{server: s}, nil
}

type fakeLink struct {
	server *fakeServer
	closed bool
}

func (l *fakeLink) Send(_ context.Context, msg *goamqp.Message) (receipt, error) {
	s := l.server
	s.mutex.Lock()
	defer s.mutex.Unlock()

	id := msg.Properties.MessageID.(string)
	if s.attempts == nil {
		s.attempts = map[string]int{}
	}
	s.attempts[id]++
	r := result{}
	if s.respond != nil {
		r = s.respond(id, s.attempts[id])
	}
	if r.sendErr != nil {
		return nil, r.sendErr
	}
	s.sent = append(s.sent, id)
	return &fakeReceipt{server: s, id: id, result: r}, nil
}

func (l *fakeLink) Close() error {
	l.closed = true
	return nil
}

type fakeReceipt struct {
	server *fakeServer
	id     string
	result result
}

func (r *fakeReceipt) Wait(context.Context) (goamqp.DeliveryState, error) {
	if r.result.waitErr != nil {
		return nil, r.result.waitErr
	}
	if r.result.state != nil {
		return r.result.state, nil
	}
	r.server.mutex.Lock()
	r.server.accepted = append(r.server.accepted, r.id)
	r.server.mutex.Unlock()
	return &goamqp.StateAccepted{}, nil
}

func newTestPublisher(server *fakeServer, config publisher.AMQPConfig) (*AMQPPublisher, *[]time.Duration) {
	sleeps := &[]time.Duration{}
	var mutex sync.Mutex
	config.NetworkID = "dev"
	return &AMQPPublisher{
		config: publisher.Config{
			Hosts: []string{"amqp://broker:5672"},
			MspId: "Org1MSP",
			AMQP:  withDefaults(config),
		},
		connect: server.connect,
		sleep: func(d time.Duration) {
			mutex.Lock()
			*sleeps = append(*sleeps, d)
			mutex.Unlock()
		},
	}, sleeps
}

func newEvents(block uint64, count int) *gateway.ChaincodeEventsResponse {
	events := &gateway.ChaincodeEventsResponse{BlockNumber: block}
	for i := 0; i < count; i++ {
		events.Events = append(events.Events, &peer.ChaincodeEvent{
			ChaincodeId: "basic",
			TxId:        fmt.Sprintf("tx-%d-%d", block, i),
			EventName:   "AssetCreated",
			Payload:     []byte("payload"),
		})
	}
	return events
}

// writeTestCertificate writes a self-signed certificate and its private key to PEM files.
func writeTestCertificate(t *testing.T) (string, string) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	require.NoError(t, os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600))
	return certFile, keyFile
}
