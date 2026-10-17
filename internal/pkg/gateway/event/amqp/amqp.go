/*
Copyright National Payments Corporation of India. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

/*
Package amqp publishes chaincode events to an AMQP 1.0 server.

Each chaincode event is sent as a durable message to a hierarchical address:

	<addressPrefix>drunix<sep><networkId><sep><channel><sep>chaincode<sep><chaincode><sep><event>

where <sep> is the configured topic separator ("/" or "."). The variable levels only contain the
characters [A-Za-z0-9_-]; any other character is replaced by "_". The original values are carried
in the message application properties.

Configuration (peer.chaincode.eventpublisher):
  - serverType: amqp
  - hosts: AMQP server urls, amqp://host:5672 or amqps://host:5671 for TLS, tried in order
  - caCertificates, clientCertificate, clientPrivateKey: TLS settings, used with amqps:// hosts
  - amqp.addressPrefix: prefix prepended to every address (default "")
  - amqp.topicSeparator: separator between address levels, "/" or "." (default "/")
  - amqp.saslMechanism: anonymous, plain or external (default plain when a username is set, otherwise anonymous)
  - amqp.username, amqp.password: credentials for the plain mechanism
  - amqp.onUndeliverable: block or skip an event the server does not accept (default block)

Delivery is at-least-once: messages are resent in block order until the server accepts them, so a
consumer can receive duplicates. The message-id, and the blockNumber and eventIndex application
properties identify each event.
*/
package amqp

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	goamqp "github.com/Azure/go-amqp"
	"github.com/hyperledger/fabric-protos-go/gateway"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/npci/drunix/common/flogging"
	"github.com/npci/drunix/common/metadata"
	"github.com/npci/drunix/internal/pkg/gateway/event/publisher"
)

var logger = flogging.MustGetLogger("amqp")

const (
	addressRoot       = "drunix"
	chaincodeCategory = "chaincode"
	defaultSeparator  = "/"
	contentTypeJSON   = "application/json"
	messagePriority   = 4

	saslAnonymous = "anonymous"
	saslPlain     = "plain"
	saslExternal  = "external"

	undeliverableBlock = "block"
	undeliverableSkip  = "skip"

	connectTimeout  = 30 * time.Second
	sendTimeout     = 30 * time.Second
	acceptTimeout   = 30 * time.Second
	minRetryBackoff = time.Second
	maxRetryBackoff = 30 * time.Second
)

// receipt reports the outcome of a message sent to the server.
type receipt interface {
	Wait(ctx context.Context) (goamqp.DeliveryState, error)
}

// link sends messages to the server.
type link interface {
	Send(ctx context.Context, msg *goamqp.Message) (receipt, error)
	Close() error
}

// connector opens a new link to the server.
type connector func(ctx context.Context) (link, error)

type AMQPPublisher struct {
	config  publisher.Config
	connect connector
	sleep   func(time.Duration)

	mutex sync.Mutex
	link  link
}

// chaincodeEvent is the message body. It has the same layout as the Kafka publisher payload.
type chaincodeEvent struct {
	BlockNumber   uint64
	TransactionID string
	ChaincodeName string
	EventName     string
	Payload       []byte
}

/*
NewAMQPPublisher

Type: Constructor

NewAMQPPublisher validates the configuration and connects to the AMQP 1.0 server.

Parameters:
  - config : event publisher configuration

Return Parameters:
  - *AMQPPublisher: the publisher, connected to the server
  - error: An error if the configuration is invalid or no server could be reached
*/
func NewAMQPPublisher(config publisher.Config) (*AMQPPublisher, error) {
	config.AMQP = withDefaults(config.AMQP)
	if err := validateConfig(config); err != nil {
		return nil, err
	}

	options, err := newConnOptions(config)
	if err != nil {
		return nil, err
	}

	p := &AMQPPublisher{
		config: config,
		connect: func(ctx context.Context) (link, error) {
			return dial(ctx, config.Hosts, options)
		},
		sleep: time.Sleep,
	}

	if _, err := p.currentLink(); err != nil {
		logger.Errorf("%s failed to connect to AMQP server %s", err, config.Hosts)
		return nil, err
	}

	return p, nil
}

/*
Publish

Type: Method

Publish sends the chaincode events of a block to the AMQP server in block order, and returns once
every event has been accepted by the server, or skipped when onUndeliverable is set to skip.

Parameters:
  - channel  : name of the channel
  - ChaincodeEventsResponse: events provided by the chaincode for a block

Return Parameters:
  - error: An error if there is an issue in handling the events
*/
func (p *AMQPPublisher) Publish(channel string, events *gateway.ChaincodeEventsResponse) (err error) {
	blockNumber := events.GetBlockNumber()

	defer func() {
		if r := recover(); r != nil {
			logger.Errorw("recovered from panic while publishing events", "channel", channel, "block", blockNumber, "panic", r)
			err = fmt.Errorf("panic while publishing events: %v", r)
		}
	}()

	messages := make([]*goamqp.Message, 0, len(events.GetEvents()))
	for index, event := range events.GetEvents() {
		msg, err := p.newMessage(channel, blockNumber, index, event)
		if err != nil {
			logger.Errorw("failed to create message for event", "channel", channel, "block", blockNumber, "eventIndex", index, "error", err)
			continue
		}
		messages = append(messages, msg)
	}

	backoff := minRetryBackoff
	for next := 0; next < len(messages); {
		accepted, err := p.send(messages[next:])
		next += accepted
		if err == nil {
			break
		}

		var undeliverable *undeliverableError
		if errors.As(err, &undeliverable) && p.config.AMQP.OnUndeliverable == undeliverableSkip {
			logger.Errorw("skipping event not accepted by AMQP server", "channel", channel, "block", blockNumber, "messageId", messages[next].Properties.MessageID, "error", err)
			next++
			backoff = minRetryBackoff
			continue
		}

		logger.Errorw("failed to publish event to AMQP server, retrying", "channel", channel, "block", blockNumber, "messageId", messages[next].Properties.MessageID, "retryIn", backoff, "error", err)
		p.sleep(backoff)
		backoff *= 2
		if backoff > maxRetryBackoff {
			backoff = maxRetryBackoff
		}
	}

	logger.Infof("completed events published for block number: %v with events length[%v]", blockNumber, len(messages))

	return nil
}

// send sends the messages in order and waits until the server settles each of them. It returns the
// number of leading messages accepted by the server, and the error for the first one that was not.
func (p *AMQPPublisher) send(messages []*goamqp.Message) (int, error) {
	l, err := p.currentLink()
	if err != nil {
		return 0, err
	}

	receipts := make([]receipt, 0, len(messages))
	var sendErr error
	for _, msg := range messages {
		logger.Debugf("sending message %v to address %s", msg.Properties.MessageID, *msg.Properties.To)
		ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
		r, err := l.Send(ctx, msg)
		cancel()
		if err != nil {
			sendErr = err
			break
		}
		receipts = append(receipts, r)
	}

	for i, r := range receipts {
		ctx, cancel := context.WithTimeout(context.Background(), acceptTimeout)
		state, err := r.Wait(ctx)
		cancel()
		if err != nil {
			p.resetLink(l)
			return i, classifyError(err)
		}
		if err := outcomeError(state); err != nil {
			return i, err
		}
	}

	if sendErr != nil {
		p.resetLink(l)
		return len(receipts), classifyError(sendErr)
	}

	return len(receipts), nil
}

// currentLink returns the open link, connecting to the server when there is none.
func (p *AMQPPublisher) currentLink() (link, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.link != nil {
		return p.link, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	l, err := p.connect(ctx)
	if err != nil {
		return nil, err
	}
	p.link = l

	return l, nil
}

// resetLink closes the link so that the next send reconnects. It does nothing if the link has
// already been replaced.
func (p *AMQPPublisher) resetLink(l link) {
	p.mutex.Lock()
	if p.link != l {
		p.mutex.Unlock()
		return
	}
	p.link = nil
	p.mutex.Unlock()

	if err := l.Close(); err != nil {
		logger.Debugf("error closing AMQP connection: %s", err)
	}
}

func (p *AMQPPublisher) newMessage(channel string, blockNumber uint64, index int, event *peer.ChaincodeEvent) (*goamqp.Message, error) {
	body, err := json.Marshal(&chaincodeEvent{
		BlockNumber:   blockNumber,
		TransactionID: event.GetTxId() + "/" + os.Getenv("CORE_PEER_ID"),
		ChaincodeName: event.GetChaincodeId(),
		EventName:     event.GetEventName(),
		Payload:       event.GetPayload(),
	})
	if err != nil {
		return nil, err
	}

	address := p.address(channel, event.GetChaincodeId(), event.GetEventName())
	eventName := event.GetEventName()
	contentType := contentTypeJSON
	groupID := channel
	creationTime := time.Now().UTC()

	return &goamqp.Message{
		Header: &goamqp.MessageHeader{
			Durable:  true,
			Priority: messagePriority,
		},
		Properties: &goamqp.MessageProperties{
			MessageID:    fmt.Sprintf("%s/%d/%d", channel, blockNumber, index),
			To:           &address,
			Subject:      &eventName,
			ContentType:  &contentType,
			GroupID:      &groupID,
			CreationTime: &creationTime,
		},
		ApplicationProperties: map[string]any{
			"channel":     channel,
			"chaincode":   event.GetChaincodeId(),
			"event":       eventName,
			"txId":        event.GetTxId(),
			"mspId":       p.config.MspId,
			"blockNumber": int64(blockNumber),
			"eventIndex":  int64(index),
		},
		Data: [][]byte{body},
	}, nil
}

// address returns the hierarchical address of a chaincode event.
func (p *AMQPPublisher) address(channel, chaincode, event string) string {
	levels := []string{
		addressRoot,
		addressLevel(p.config.AMQP.NetworkID),
		addressLevel(channel),
		chaincodeCategory,
		addressLevel(chaincode),
		addressLevel(event),
	}
	return p.config.AMQP.AddressPrefix + strings.Join(levels, p.config.AMQP.TopicSeparator)
}

// addressLevel replaces every character outside [A-Za-z0-9_-] with "_", so that a value can never
// add address levels or wildcards.
func addressLevel(value string) string {
	if value == "" {
		return "_"
	}
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-':
			return r
		default:
			return '_'
		}
	}, value)
}

// undeliverableError is returned for a message the server will not accept, such as a rejected,
// released or oversized message.
type undeliverableError struct {
	reason string
	cause  error
}

func (e *undeliverableError) Error() string {
	if e.cause == nil {
		return "message " + e.reason
	}
	return fmt.Sprintf("message %s: %s", e.reason, e.cause)
}

func (e *undeliverableError) Unwrap() error {
	return e.cause
}

// outcomeError returns nil if the server accepted the message, an undeliverableError if the server
// will not accept it, and any other error if the message should be sent again.
func outcomeError(state goamqp.DeliveryState) error {
	switch s := state.(type) {
	case *goamqp.StateAccepted:
		return nil
	case *goamqp.StateRejected:
		if s.Error == nil {
			return &undeliverableError{reason: "rejected"}
		}
		return &undeliverableError{reason: "rejected", cause: s.Error}
	case *goamqp.StateReleased:
		return &undeliverableError{reason: "released"}
	case *goamqp.StateModified:
		if s.UndeliverableHere {
			return &undeliverableError{reason: "modified as undeliverable"}
		}
		return errors.New("message modified")
	default:
		return fmt.Errorf("unexpected delivery state %T", state)
	}
}

// classifyError marks errors for messages that exceed the maximum message size as undeliverable.
func classifyError(err error) error {
	var amqpErr *goamqp.Error
	if errors.As(err, &amqpErr) && amqpErr.Condition == goamqp.ErrCondMessageSizeExceeded {
		return &undeliverableError{reason: "too large", cause: err}
	}
	return err
}

func withDefaults(c publisher.AMQPConfig) publisher.AMQPConfig {
	if c.TopicSeparator == "" {
		c.TopicSeparator = defaultSeparator
	}
	c.SASLMechanism = strings.ToLower(c.SASLMechanism)
	if c.SASLMechanism == "" {
		c.SASLMechanism = saslAnonymous
		if c.Username != "" {
			c.SASLMechanism = saslPlain
		}
	}
	c.OnUndeliverable = strings.ToLower(c.OnUndeliverable)
	if c.OnUndeliverable == "" {
		c.OnUndeliverable = undeliverableBlock
	}
	return c
}

func validateConfig(config publisher.Config) error {
	if len(config.Hosts) == 0 {
		return errors.New("no broker url passed")
	}

	tlsConfigured := config.ClientCertificate != "" || config.ClientPrivateKey != "" || len(config.CACertificates) > 0
	for i, host := range config.Hosts {
		u, err := url.Parse(host)
		if err != nil {
			return fmt.Errorf("invalid AMQP host at position %d: %w", i, err)
		}
		if u.User != nil {
			return fmt.Errorf("AMQP host at position %d contains credentials, set amqp.username and amqp.password instead", i)
		}
		switch u.Scheme {
		case "amqps":
		case "amqp":
			if tlsConfigured {
				return fmt.Errorf("TLS certificates are configured but AMQP host %s does not use amqps://", host)
			}
		default:
			return fmt.Errorf("AMQP host %s must use the amqp:// or amqps:// scheme", host)
		}
		if u.Hostname() == "" {
			return fmt.Errorf("AMQP host %s has no host name", host)
		}
	}

	c := config.AMQP
	if c.TopicSeparator != "/" && c.TopicSeparator != "." {
		return fmt.Errorf("invalid AMQP topic separator %q, must be \"/\" or \".\"", c.TopicSeparator)
	}
	if strings.ContainsAny(c.AddressPrefix, "*>#+") {
		return fmt.Errorf("AMQP address prefix %q must not contain wildcard characters", c.AddressPrefix)
	}

	switch c.SASLMechanism {
	case saslAnonymous:
	case saslPlain:
		if c.Username == "" {
			return errors.New("AMQP SASL mechanism plain requires a username")
		}
	case saslExternal:
		if config.ClientCertificate == "" || config.ClientPrivateKey == "" {
			return errors.New("AMQP SASL mechanism external requires a client certificate and private key")
		}
	default:
		return fmt.Errorf("unsupported AMQP SASL mechanism %q", c.SASLMechanism)
	}

	switch c.OnUndeliverable {
	case undeliverableBlock, undeliverableSkip:
	default:
		return fmt.Errorf("invalid AMQP onUndeliverable value %q, must be block or skip", c.OnUndeliverable)
	}

	return nil
}

func newConnOptions(config publisher.Config) (*goamqp.ConnOptions, error) {
	options := &goamqp.ConnOptions{
		Properties: map[string]any{
			"product": "drunix",
			"version": metadata.Version,
		},
	}

	switch config.AMQP.SASLMechanism {
	case saslPlain:
		options.SASLType = goamqp.SASLTypePlain(config.AMQP.Username, config.AMQP.Password)
	case saslExternal:
		options.SASLType = goamqp.SASLTypeExternal("")
	default:
		options.SASLType = goamqp.SASLTypeAnonymous()
	}

	tlsConfig, err := newTLSConfig(config)
	if err != nil {
		return nil, err
	}
	options.TLSConfig = tlsConfig

	return options, nil
}

func newTLSConfig(config publisher.Config) (*tls.Config, error) {
	if config.ClientCertificate == "" && config.ClientPrivateKey == "" && len(config.CACertificates) == 0 {
		return nil, nil
	}

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}

	if len(config.CACertificates) > 0 {
		pool := x509.NewCertPool()
		for _, file := range config.CACertificates {
			pem, err := os.ReadFile(file)
			if err != nil {
				return nil, fmt.Errorf("failed to read CA certificate %s: %w", file, err)
			}
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("no certificates found in %s", file)
			}
		}
		tlsConfig.RootCAs = pool
	}

	if config.ClientCertificate != "" || config.ClientPrivateKey != "" {
		cert, err := tls.LoadX509KeyPair(config.ClientCertificate, config.ClientPrivateKey)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

// dial connects to the first reachable host.
func dial(ctx context.Context, hosts []string, options *goamqp.ConnOptions) (link, error) {
	var errs []error
	for _, host := range hosts {
		l, err := dialHost(ctx, host, options)
		if err == nil {
			logger.Infof("connected to AMQP server %s", host)
			return l, nil
		}
		logger.Warnw("failed to connect to AMQP server", "host", host, "error", err)
		errs = append(errs, err)
	}
	return nil, fmt.Errorf("failed to connect to AMQP server: %w", errors.Join(errs...))
}

func dialHost(ctx context.Context, host string, options *goamqp.ConnOptions) (link, error) {
	opts := *options
	opts.ContainerID = containerID()

	conn, err := goamqp.Dial(ctx, host, &opts)
	if err != nil {
		return nil, err
	}

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		conn.Close()
		return nil, err
	}

	// An empty target creates an anonymous sender: each message is routed by its "to" address.
	sender, err := session.NewSender(ctx, "", &goamqp.SenderOptions{
		SettlementMode: goamqp.SenderSettleModeUnsettled.Ptr(),
	})
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &amqpLink{conn: conn, sender: sender}, nil
}

// containerID identifies the peer to the server. The random suffix keeps it unique across
// reconnections.
func containerID() string {
	peerID := os.Getenv("CORE_PEER_ID")
	if peerID == "" {
		peerID = "peer"
	}
	hostname, _ := os.Hostname()
	suffix := make([]byte, 4)
	_, _ = rand.Read(suffix)
	return fmt.Sprintf("%s-%s-%s", peerID, hostname, hex.EncodeToString(suffix))
}

// amqpLink is a link backed by an AMQP connection with a single anonymous sender.
type amqpLink struct {
	conn   *goamqp.Conn
	sender *goamqp.Sender
}

func (l *amqpLink) Send(ctx context.Context, msg *goamqp.Message) (receipt, error) {
	r, err := l.sender.SendWithReceipt(ctx, msg, nil)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// Close closes the connection, which also closes the session and the sender.
func (l *amqpLink) Close() error {
	return l.conn.Close()
}
