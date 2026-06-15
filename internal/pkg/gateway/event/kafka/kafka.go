
/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/
package kafka

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/hyperledger/fabric-protos-go/gateway"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/npci/drunix/common/flogging"
	"github.com/npci/drunix/internal/pkg/gateway/event/publisher"
)

var logger = flogging.MustGetLogger("kafka")

type KafkaPublisher struct {
	producer sarama.SyncProducer
	config   publisher.Config
}

/*
NewKafkaPublisher

Type: Constructor

NewCacheAdapter initializes a new kafka Publisher to publish event based on the provided by chaincode.

Parameters:
  - Kafkaurls : an array of strings that contains endpoints of the urls

Return Parameters:
  - event.Publisher: An interface representing the initialized Publisher
  - error: An error if there is an issue in creating  the cache Publisher
*/
func NewKafkaPublisher(kafkaconfig publisher.Config) (*KafkaPublisher, error) {
	if len(kafkaconfig.Hosts) == 0 {
		return nil, fmt.Errorf("no broker url passed")
	}

	config := sarama.NewConfig()
	// config.Producer.Return.Successes = true
	// config.Producer.RequiredAcks = sarama.WaitForAll
	// config.Version = sarama.MaxVersion
	// config.Producer.Retry.Max = 5
	// config.Producer.Idempotent = true

	// config.Version = ver
	// This config is similar to eventlistener
	// ************ config changes****************
	maxOpenReq := 1
	maxMessages := 16 * 1024
	flushFreq := 100 * time.Millisecond
	idempotencyEnabled := true

	if os.Getenv("MAX_OPEN_REQUEST") != "" {
		maxOpenReq, _ = strconv.Atoi(os.Getenv("MAX_OPEN_REQUEST"))
	}
	if os.Getenv("PRODUCER_MAX_MESSAGES") != "" {
		maxMessages, _ = strconv.Atoi(os.Getenv("PRODUCER_MAX_MESSAGES"))
	}

	if os.Getenv("FLUSH_FREQUENCY") != "" {
		flushFreqEnv, err := strconv.Atoi(os.Getenv("FLUSH_FREQUENCY"))
		if err != nil {
			logger.Errorf("Failed reading Kafka FLUSH_FREQUENCY. err:%+v", err)

		} else {
			flushFreq = time.Duration(flushFreqEnv) * time.Millisecond
		}

	}
	if os.Getenv("IDEMPOTENCY_ENABLED") != "" {
		idempotencyEnabled, _ = strconv.ParseBool(os.Getenv("IDEMPOTENCY_ENABLED"))
	}
	// ************ config changes****************

	config.ClientID = "peer"
	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 3
	config.Producer.Idempotent = idempotencyEnabled
	config.Producer.Timeout = time.Second * 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Net.MaxOpenRequests = maxOpenReq
	// config.Producer.Flush.Frequency = 100 * time.Millisecond
	config.Producer.Flush.Frequency = flushFreq
	// config.Producer.Flush.MaxMessages = 200000 * 1024
	config.Producer.Flush.MaxMessages = maxMessages
	config.Producer.Compression = sarama.CompressionLZ4
	// config.Producer.MaxMessageBytes =

	// config.ChannelBufferSize =
	// config.Producer.MaxMessageBytes
	// config.Version = sarama.V3_3_1_0
	config.Version = sarama.MaxVersion

	err := CreateTopicsIfNotExists(config, kafkaconfig.Hosts, kafkaconfig.Topics)
	if err != nil {
		logger.Errorf("%s failed to create topics for kafkaUrl %s", err, kafkaconfig.Hosts)
		return nil, err
	}

	producer, err := sarama.NewSyncProducer(kafkaconfig.Hosts, config)
	if err != nil {
		logger.Errorf("%s failed to create Producer for kafkaUrl %s", err, kafkaconfig.Hosts)
		return nil, err
	}

	return &KafkaPublisher{producer: producer, config: kafkaconfig}, nil
}

func CreateTopicsIfNotExists(config *sarama.Config, brokers []string, topics []string) error {
	// s.log.InfoWithContext(s.ctx, KafkaProducerService,
	// "CreateTopicsIfNotExists(): started creating kafka topics if not exists")

	// config := sarama.NewConfig()
	// config.Producer.Return.Successes = true
	// config.Consumer.Offsets.Initial = sarama.OffsetNewest
	// config.Version = sarama.MaxVersion
	// config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategySticky}
	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		return fmt.Errorf("error while creating kafka admin %w", err)
	}
	err = createTopics(topics, admin)
	if err != nil {
		return err
	}

	return nil
}

func createTopics(topics []string, admin sarama.ClusterAdmin) error {
	logger.Infof("started creating kafka topics")
	existingTopics, err := admin.ListTopics()
	if err != nil {
		return fmt.Errorf("failed to list topics %w", err)
	}

	for _, topic := range topics {
		if _, ok := existingTopics[topic]; !ok {
			err := admin.CreateTopic(topic, &sarama.TopicDetail{
				NumPartitions:     2,
				ReplicationFactor: 1,
			}, false)
			if err != nil {
				logger.Errorf("error while creating topic: %v", err)
				continue
			}
		}
	}

	listTopics, err := admin.ListTopics()
	if err != nil {
		return err
	}
	createdTopics := make([]string, 0)
	for _, topic := range topics {
		if _, ok := listTopics[topic]; !ok {
		} else {
			createdTopics = append(createdTopics, topic)
		}
	}
	logger.Infof("List of topics created: %v", createdTopics)

	return nil
}

/*
Publish

Type: Method

Publish: publish event based on the provided by chaincode to kafka broker.

Parameters:
  - channel  : name of the channel
  - ChaincodeEventsResponse: events provided by the chaincode for a block

Return Parameters:
- error: An error if there is an issue in handling the events
*/

// This payload is used by TPU for unmarshalling the events
type ChaincodeEvent struct {
	BlockNumber   uint64
	TransactionID string
	ChaincodeName string
	EventName     string
	Payload       []byte
}

func (input *KafkaPublisher) Publish(channel string, events *gateway.ChaincodeEventsResponse) error {
	blockId := events.GetBlockNumber()
	// processedEvents := 0

	totalEvenets := len(events.GetEvents())

	wg := &sync.WaitGroup{}

	for _, event := range events.GetEvents() {
		// if input.config.ShardEnabled && input.skipPublish(channel, transactionShards[event.GetTxId()]) {
		// 	logger.Debugf("skipped publishing [ChaincodeId : %s], [TxnId : %s] to kafka", event.ChaincodeId, event.TxId)
		// 	continue
		// }

		wg.Add(1)

		go func(event *peer.ChaincodeEvent, totalEvenets int) {

			defer wg.Done()

			ccEvent := &ChaincodeEvent{
				BlockNumber:   blockId,
				TransactionID: fmt.Sprint(event.GetTxId() + "/" + os.Getenv("CORE_PEER_ID")),
				ChaincodeName: event.GetChaincodeId(),
				EventName:     event.GetEventName(),
				Payload:       event.GetPayload(),
			}

			ccEventBytes, err := json.Marshal(ccEvent)
			if err != nil {
				logger.Errorw("error marshalling event", "event", event, "error", err)
				return
			}

			topicName := fmt.Sprintf("%s_%s", channel, event.GetChaincodeId())

			// byteEncode := make(sarama.ByteEncoder, 0)
			// byteEncode = append(byteEncode, eventBytes...)
			// msg := &sarama.ProducerMessage{
			// 	Key:   sarama.StringEncoder(event.TxId),
			// 	Value: byteEncode,
			// 	Topic: topic,
			// }
			logger.Debugf("payload size :%v", len(ccEventBytes))
			msg := &sarama.ProducerMessage{
				Topic:     topicName,
				Key:       sarama.StringEncoder(event.EventName),
				Value:     sarama.StringEncoder(string(ccEventBytes)),
				Timestamp: time.Now().UTC(),
			}

			// if shouldEventPushToQueue(event.EventName, input.config, payload) {
			logger.Debug("pushing the event to queue")
			partition, offset, err := input.producer.SendMessage(msg)
			if err != nil {
				logger.Errorw("failed to send message to broker for event", event, "topic", topicName, "error", err)
				// closing the producer for avoiding memory leakage
				input.producer.Close()
				return
			}
			// processedEvents++
			logger.Debugf("Message is stored in topic(%s)/partition(%d)/offset(%d) for event:[%s]\n", topicName, partition, offset, event.EventName)
			// } else {
			// 	logger.Debugf("ignoring event: [%v] for orgType: %v", event.EventName, input.config.OrgType)
			// }

		}(event, totalEvenets)

	}

	wg.Wait()

	// logger.Infow("total published events", "channel", channel, "blockId", blockId, "processedEvents", processedEvents, "ignoredEvents", len(events.Events)-processedEvents)

	logger.Infof("completed events published for block number: %v with events lenght[%v]", blockId, totalEvenets)

	return nil
}

// func (input *KafkaPublisher) skipPublish(channelId string, shardId int32) bool {
// 	shardchange := input.shardChangeReporter.Get(channelId)

// 	// virtual_sharding: we are not assigning 0 as shard id in any transaction.
// 	// Hence if a txn is having shard id 0, then its a system transaction or a transaction from vanilla sdk client.
// 	// So it can't be skipped.
// 	if shardId == 0 {
// 		return false
// 	}

// 	// if number of shards available in org is less than preferred shardId, choose the modulas(remainder) of total shards count.
// 	if numOfShards := int32(input.shardChangeReporter.ShardCount(channelId)); numOfShards < shardId {
// 		shardId = shardId % numOfShards
// 		// choose shard-1 if remainder is 0 as shard-0 doesn't exist
// 		if shardId == 0 {
// 			shardId = 1
// 		}
// 	}

// 	nonShardMembers := shardchange.NonShardMembers(shardId)
// 	// check if this peer is present in the non shard members list, then skip publishing
// 	for _, member := range nonShardMembers.GetOrganizationMembers() {
// 		if member.InternalEndpoint == input.config.PeerEndpoint {
// 			// logger.Debugf("skipping validation for transaction as it belongs to shard %d with member.Endpoint:%s & v.externalEndpoint:%s", shardId, member.Endpoint, input.config.PeerEndpoint)
// 			return true
// 		}
// 	}
// 	// logger.Debugf("validating transaction as it belongs to shard %d", shardId)
// 	return false
// }
