/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0

Modifications Copyright National Payments Corporation of India
*/

package blockcutter

import (
	"time"

	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/npci/drunix/common/channelconfig"
	"github.com/npci/drunix/common/flogging"
)

var logger = flogging.MustGetLogger("orderer.common.blockcutter")

type OrdererConfigFetcher interface {
	OrdererConfig() (channelconfig.Orderer, bool)
}

// Receiver defines a sink for the ordered broadcast messages
type Receiver interface {
	// Ordered should be invoked sequentially as messages are ordered
	// Each batch in `messageBatches` will be wrapped into a block.
	// `pending` indicates if there are still messages pending in the receiver.
	Ordered(msg *cb.Envelope) (messageBatches [][]*cb.Envelope, pending bool)

	// Cut returns the current batch and starts a new one
	Cut() []*cb.Envelope
	OrderedBatch(req *orderer.SubmitRequest) (messageBatches []*orderer.Batch, pending bool)

	// Cut returns the current batch and starts a new one
	CutBatch() *orderer.Batch
}

type receiver struct {
	sharedConfigFetcher   OrdererConfigFetcher
	pendingBatch          []*cb.Envelope
	pendingFollowerBatch  *orderer.Batch
	pendingBatchSizeBytes uint32

	PendingBatchStartTime time.Time
	ChannelID             string
	Metrics               *Metrics
	// BlockCutTime int
	MaxBatchSize uint32
}

// CutBatch implements Receiver.
func (r *receiver) CutBatch() *orderer.Batch {
	if r.pendingBatch != nil && len(r.pendingFollowerBatch.Reqs) > 0 {
		r.Metrics.BlockFillDuration.With("channel", r.ChannelID).Observe(time.Since(r.PendingBatchStartTime).Seconds())
	}

	batch := &orderer.Batch{
		Reqs: r.pendingFollowerBatch.Reqs,
	}

	r.pendingFollowerBatch.Reqs = []*orderer.SubmitRequest{}
	r.PendingBatchStartTime = time.Time{}
	r.pendingBatchSizeBytes = 0
	return batch
}

// OrderedBatch implements Receiver.
func (r *receiver) OrderedBatch(req *orderer.SubmitRequest) (messageBatches []*orderer.Batch, pending bool) {
	logger.Debug("Entering Orderer ")
	if len(r.pendingFollowerBatch.Reqs) == 0 {
		// We are beginning a new batch, mark the time
		logger.Infof("len(r.pendingBatch.Reqs) == 0 , %v", len(r.pendingFollowerBatch.Reqs) == 0)

		r.PendingBatchStartTime = time.Now()
	}

	ordererConfig, ok := r.sharedConfigFetcher.OrdererConfig()
	if !ok {
		logger.Panicf("Could not retrieve orderer config to query batch parameters, block cutting is not possible")
	}

	batchSize := ordererConfig.BatchSize()
	if r.MaxBatchSize != 0 {
		batchSize.MaxMessageCount = r.MaxBatchSize
	} else {
		logger.Debug("empty MAX_BATCH_SIZE env value")
	}
	logger.Debugf("batch size : %+v\n", batchSize)

	messageSizeBytes := messageSizeBytes(req.Payload)

	if messageSizeBytes > batchSize.PreferredMaxBytes {
		logger.Infof("The current message, with %v bytes, is larger than the preferred batch size of %v bytes and will be isolated.", messageSizeBytes, batchSize.PreferredMaxBytes)

		// cut pending batch, if it has any messages
		if len(r.pendingFollowerBatch.Reqs) > 0 {
			logger.Infof(" As the len(r.pendingBatch.Reqs) is > 0, cutting the batch")
			messageBatch := r.CutBatch()
			messageBatches = append(messageBatches, messageBatch)
		}

		// create new batch with single message
		newBatch := &orderer.Batch{Reqs: []*orderer.SubmitRequest{}}
		newBatch.Reqs = append(newBatch.Reqs, req)
		messageBatches = append(messageBatches, newBatch)

		// messageBatches = append(messageBatches, []*cb.Envelope{lightMsg})

		// Record that this batch took no time to fill
		r.Metrics.BlockFillDuration.With("channel", r.ChannelID).Observe(0)

		return
	}

	messageWillOverflowBatchSizeBytes := r.pendingBatchSizeBytes+messageSizeBytes > batchSize.PreferredMaxBytes

	if messageWillOverflowBatchSizeBytes {
		logger.Infof("The current message, with %v bytes, will overflow the pending batch of %v bytes.", messageSizeBytes, r.pendingBatchSizeBytes)
		logger.Infof("Pending batch would overflow if current message is added, cutting batch now.")
		messageBatch := r.CutBatch()
		r.PendingBatchStartTime = time.Now()
		messageBatches = append(messageBatches, messageBatch)
	}

	logger.Debugf("Enqueuing message into batch")
	// r.pendingBatch = append(r.pendingBatch, msg)
	r.pendingFollowerBatch.Reqs = append(r.pendingFollowerBatch.Reqs, req)

	r.pendingBatchSizeBytes += messageSizeBytes
	pending = true

	if uint32(len(r.pendingFollowerBatch.Reqs)) >= batchSize.MaxMessageCount {
		logger.Infof("Batch size met, cutting batch")
		messageBatch := r.CutBatch()
		messageBatches = append(messageBatches, messageBatch)
		pending = false
	}

	return

}

// NewReceiverImpl creates a Receiver implementation based on the given configtxorderer manager
func NewReceiverImpl(channelID string, sharedConfigFetcher OrdererConfigFetcher, metrics *Metrics, maxBatchSize uint32) Receiver {
	return &receiver{
		sharedConfigFetcher:  sharedConfigFetcher,
		pendingFollowerBatch: &orderer.Batch{},
		Metrics:              metrics,
		ChannelID:            channelID,
		MaxBatchSize:         maxBatchSize,
	}
}

// Ordered should be invoked sequentially as messages are ordered
//
// messageBatches length: 0, pending: false
//   - impossible, as we have just received a message
//
// messageBatches length: 0, pending: true
//   - no batch is cut and there are messages pending
//
// messageBatches length: 1, pending: false
//   - the message count reaches BatchSize.MaxMessageCount
//
// messageBatches length: 1, pending: true
//   - the current message will cause the pending batch size in bytes to exceed BatchSize.PreferredMaxBytes.
//
// messageBatches length: 2, pending: false
//   - the current message size in bytes exceeds BatchSize.PreferredMaxBytes, therefore isolated in its own batch.
//
// messageBatches length: 2, pending: true
//   - impossible
//
// Note that messageBatches can not be greater than 2.
func (r *receiver) Ordered(msg *cb.Envelope) (messageBatches [][]*cb.Envelope, pending bool) {
	if len(r.pendingBatch) == 0 {
		// We are beginning a new batch, mark the time
		r.PendingBatchStartTime = time.Now()
	}

	ordererConfig, ok := r.sharedConfigFetcher.OrdererConfig()
	if !ok {
		logger.Panicf("Could not retrieve orderer config to query batch parameters, block cutting is not possible")
	}

	batchSize := ordererConfig.BatchSize()
	if r.MaxBatchSize != 0 {
		batchSize.MaxMessageCount = r.MaxBatchSize
	} else {
		logger.Debug("empty MAX_BATCH_SIZE env value")
	}
	messageSizeBytes := messageSizeBytes(msg)
	if messageSizeBytes > batchSize.PreferredMaxBytes {
		logger.Debugf("The current message, with %v bytes, is larger than the preferred batch size of %v bytes and will be isolated.", messageSizeBytes, batchSize.PreferredMaxBytes)

		// cut pending batch, if it has any messages
		if len(r.pendingBatch) > 0 {
			messageBatch := r.Cut()
			messageBatches = append(messageBatches, messageBatch)
		}

		// create new batch with single message
		messageBatches = append(messageBatches, []*cb.Envelope{msg})

		// Record that this batch took no time to fill
		r.Metrics.BlockFillDuration.With("channel", r.ChannelID).Observe(0)

		return
	}

	messageWillOverflowBatchSizeBytes := r.pendingBatchSizeBytes+messageSizeBytes > batchSize.PreferredMaxBytes

	if messageWillOverflowBatchSizeBytes {
		logger.Debugf("The current message, with %v bytes, will overflow the pending batch of %v bytes.", messageSizeBytes, r.pendingBatchSizeBytes)
		logger.Debugf("Pending batch would overflow if current message is added, cutting batch now.")
		messageBatch := r.Cut()
		r.PendingBatchStartTime = time.Now()
		messageBatches = append(messageBatches, messageBatch)
	}

	logger.Debugf("Enqueuing message into batch")
	r.pendingBatch = append(r.pendingBatch, msg)
	r.pendingBatchSizeBytes += messageSizeBytes
	pending = true

	if uint32(len(r.pendingBatch)) >= batchSize.MaxMessageCount {
		logger.Debugf("Batch size met, cutting batch")
		messageBatch := r.Cut()
		messageBatches = append(messageBatches, messageBatch)
		pending = false
	}

	return
}

// Cut returns the current batch and starts a new one
func (r *receiver) Cut() []*cb.Envelope {
	if r.pendingBatch != nil {
		r.Metrics.BlockFillDuration.With("channel", r.ChannelID).Observe(time.Since(r.PendingBatchStartTime).Seconds())
	}
	r.PendingBatchStartTime = time.Time{}
	batch := r.pendingBatch
	r.pendingBatch = nil
	r.pendingBatchSizeBytes = 0
	return batch
}

func messageSizeBytes(message *cb.Envelope) uint32 {
	return uint32(len(message.Payload) + len(message.Signature))
}
