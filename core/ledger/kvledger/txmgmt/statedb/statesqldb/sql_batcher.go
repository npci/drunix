/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/

package statesqldb

import (
	"fmt"
	"time"

	"github.com/npci/drunix/consts"
	"gorm.io/gorm"
)

/*
DRUNIX:
	This file contains code for:
		- initalizing and starting the sql batch listener. this will batch all the keys to retrieve and after a pre-defined time interval an sql scan operation is performed on the keys and the relevant values are fetched.
*/

type ResponseChan struct {
	Value DBValue
	Error error
}

type SqlBatcher interface {
	Get(key string) (DBValue, error)
}

type sqlBatcher struct {
	channel chan map[string]chan ResponseChan
	batch   map[string]chan ResponseChan
	*sqlSchema
}

var (
	channelBufferSize = consts.ENDORSER_BATCH_CHANNEL_BUFFER
	batchInterval     = time.Duration(consts.ENDORSER_BATCH_INTERVAL) * time.Millisecond
)

// DRUNIX: initialize sql batcher with the pre-defined buffer size and start the batch listener in a go routine
func NewSqlBatcher(sqlSchema *sqlSchema) SqlBatcher {

	logger.Info("NewSqlBatcher Initialised")

	sb := &sqlBatcher{
		channel:   make(chan map[string]chan ResponseChan, channelBufferSize),
		batch:     make(map[string]chan ResponseChan, 0),
		sqlSchema: sqlSchema,
	}

	go sb.startBatchListener()

	return sb
}

// DRUNIX: whenever a value for a key needs to be fetched from sql the key and the respnse channel is sent to batch channel.
// the response channel returns the value or error
func (sb *sqlBatcher) Get(key string) (DBValue, error) {

	// defer TimeIt("SqlBatcher Get")()

	responseChan := make(chan ResponseChan, 1)

	select {
	case sb.channel <- map[string]chan ResponseChan{key: responseChan}:
		response := <-responseChan
		close(responseChan)
		return response.Value, response.Error
	default:
		return DBValue{}, fmt.Errorf("failed to queue key-value pair : %s", key)
	}
}

// DRUNIX: listen infintely on the batch channel and store then keys and the relevant response channel in a map. After the specified time interval the data in the batch map is dumped to a flush map and process that flush map
func (sb *sqlBatcher) startBatchListener() {

	ticker := time.NewTicker(batchInterval)

	defer ticker.Stop()

	for {
		select {
		case kv := <-sb.channel:
			for key, value := range kv {
				sb.batch[key] = value
			}
		case <-ticker.C:
			rbBatchLen := len(sb.batch)
			if rbBatchLen > 0 {
				batchToFlush := make(map[string]chan ResponseChan, rbBatchLen)
				for key, responseChan := range sb.batch {
					batchToFlush[key] = responseChan
				}
				go sb.flushBatchToSql(batchToFlush)
				sb.batch = make(map[string]chan ResponseChan, 0)
			}
		}
	}

}

// DRUNIX: get the keys from the flush map and get the relevant table. Scan the keys and retrieve values and send them in the response channel
func (sb *sqlBatcher) flushBatchToSql(batchToFlush map[string]chan ResponseChan) {

	// defer TimeIt("Sql flushBatchToSql")()

	errKeys := map[string]error{}

	batchData := map[string][][]byte{}
	for key := range batchToFlush {
		table, _, err := sb.sqlSchema.GetTable(key)
		if err != nil {
			errKeys[key] = err
			continue
		}
		batchData[table] = append(batchData[table], []byte(key))
	}

	for errKey, err := range errKeys {
		batchToFlush[errKey] <- ResponseChan{Error: err}
		delete(batchToFlush, errKey)
	}

	for table, batch := range batchData {
		keysInTable := make(map[string]struct{})
		for _, key := range batch {
			keysInTable[string(key)] = struct{}{}
		}
		var results []DBValue
		if err := sb.sqlSchema.Client.Table(table).Select("key", "block_number", "transaction_number", "db_metadata", "db_value").Where("key IN ?", batch).Scan(&results).Error; err != nil {
			for key := range keysInTable {
				if ch, ok := batchToFlush[key]; ok {
					ch <- ResponseChan{Error: err}
					delete(batchToFlush, key)
				}
			}
			continue
		}
		for _, data := range results {
			key := string(data.Key)
			if responseChan, exists := batchToFlush[key]; exists {
				responseChan <- ResponseChan{Value: data}
				delete(batchToFlush, key)
			}
		}
	}

	for _, responseChan := range batchToFlush {
		responseChan <- ResponseChan{Error: gorm.ErrRecordNotFound}
	}
}
