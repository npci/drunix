/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/

package transientstore

import (
	"context"
	"fmt"
	"time"

	// "github.com/npci/drunix/consts"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
)

/*
DRUNIX: Since this file is a custom addition this comment explains the process happening in the file.
	- This file is used to batch the transient data writes and insert them to key-value db
	- Initialize the batcher with a `KeyValue` channel and a redis client, start listening to batches in a go routine
	- whenever private data is needed to be inserted into key-value db `Set` method of the batcher is invoked
	- The batcher continously listens to write requests and batches them. After a specified time interval(`ENDORSER_BATCH_INTERVAL`) the batch is written to key-value db
	- we are not retrying in case of batch insertion failures since the error is back-propagated to endorsement call and the transaction fails. So the txn is not sent to orderer.
	- gathering metrics for total write requests and wrtite failure count, also write processing time.

	Environment variables used:
		-> peer.endorserbatchchannelbuffer - time interval to process the batch
		-> peer.endorserbatchinterval - channel buffer size

	TODO:
		- as of now we are processing the batch after a specified time interval. We have to handle a case where we need to process the batch when the buffer is full before the time interval is done.
*/

type KeyValue struct {
	Key          string
	Value        map[string]string
	ResponseChan chan error
}

type KVStoreBatcher interface {
	Set(key string, value map[string]string) error
}

type kvStoreBatcher struct {
	channel       chan KeyValue
	store         *Store
	batch         []KeyValue
	batchInterval time.Duration
}

func newKVStoreBatcher(store *Store) KVStoreBatcher {

	channelBufferSize := 2000
	if viper.IsSet("peer.endorserbatchchannelbuffer") {
		channelBufferSize = viper.GetInt("peer.endorserbatchchannelbuffer")
	}

	batchInterval := 100 * time.Millisecond
	if viper.IsSet("peer.endorserbatchinterval") {
		batchInterval = viper.GetDuration("peer.endorserbatchinterval")
	}

	kb := &kvStoreBatcher{
		channel:       make(chan KeyValue, channelBufferSize),
		store:         store,
		batch:         make([]KeyValue, 0),
		batchInterval: batchInterval,
	}

	go kb.startBatchListener()

	return kb
}

func (kb *kvStoreBatcher) Set(key string, value map[string]string) error {

	responseChan := make(chan error, 1)

	kv := KeyValue{
		Key:          key,
		Value:        value,
		ResponseChan: responseChan,
	}

	select {
	case kb.channel <- kv:
		err := <-responseChan
		close(responseChan)
		return err
	default:
		return fmt.Errorf("failed to queue key-value pair : %s", key)
	}
}

func (kb *kvStoreBatcher) startBatchListener() {

	ticker := time.NewTicker(kb.batchInterval)

	defer ticker.Stop()

	for {
		select {
		case kv := <-kb.channel:
			kb.batch = append(kb.batch, kv)
		case <-ticker.C:
			kbBatchLen := len(kb.batch)
			if kbBatchLen > 0 {
				batchToFlush := make([]KeyValue, kbBatchLen)
				for i, kv := range kb.batch {
					batchToFlush[i] = KeyValue{
						Key:          kv.Key,
						Value:        kv.Value,
						ResponseChan: kv.ResponseChan, // Channels are references, so this will point to the same channel
					}
				}
				go kb.flushBatchToRedis(batchToFlush)
				kb.batch = []KeyValue{}
			}
		}
	}

}

func (kb *kvStoreBatcher) flushBatchToRedis(batchToFlush []KeyValue) {

	// defer TimeIt("TransientStore flushBatchToRedis")()

	logger.Debug("Flush TransientStore Batch Len : ", len(batchToFlush))

	errMap := map[int]struct{}{}
	pipe := kb.store.KVStore.db.Client.Pipeline()
	ctx := context.Background()

	/*
		DRUNIX
		Fetching the min height of the existing CPs
		and setting the txId along with the min block height,
		so that txns won't be purged by the leading peer,
		and lagging peer won't have to populate pvtData from remote peer
	*/
	_, minHeight, err := kb.store.KVStore.GetMinHeight()
	if err != nil {
		for _, kv := range batchToFlush {
			kv.ResponseChan <- fmt.Errorf("batch failure: %v ", err)
		}
		return
	}

	logger.Infof("Setting Pvt Data at Height of %v", minHeight)

	zSetTxns := make([]redis.Z, len(batchToFlush))

	for index, kv := range batchToFlush {
		err := pipe.HSet(ctx, kv.Key, kv.Value).Err()
		if err != nil {
			kv.ResponseChan <- fmt.Errorf("batch failure: %v ", err)
			errMap[index] = struct{}{}
		}
		zSetTxns[index] = redis.Z{
			Score:  minHeight,
			Member: kv.Key,
		}
	}

	err = pipe.ZAdd(ctx, fmt.Sprintf("%s:txns_by_block", kb.store.KVStore.ledgerID), zSetTxns...).Err()
	if err != nil {
		for index, kv := range batchToFlush {
			if _, exists := errMap[index]; exists {
				continue
			}
			kv.ResponseChan <- fmt.Errorf("batch failure: %v ", err)
		}
		return
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		for index, kv := range batchToFlush {
			if _, exists := errMap[index]; exists {
				continue
			}
			kv.ResponseChan <- fmt.Errorf("batch failure: %v ", err)
		}
		return
	} else {
		for index, kv := range batchToFlush {
			if _, exists := errMap[index]; exists {
				continue
			}
			kv.ResponseChan <- nil
		}
		return
	}
}
