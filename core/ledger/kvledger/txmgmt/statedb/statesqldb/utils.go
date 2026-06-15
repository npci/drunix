/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/

package statesqldb

import (
	"bytes"
	"sync"
	"time"

	"github.com/npci/drunix/core/ledger/internal/version"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/statedb"
)

func encodeDataKey(ns, key string) []byte {
	k := append(dataKeyPrefix, []byte(ns)...)
	k = append(k, nsKeySep...)
	return append(k, []byte(key)...)
}

func decodeDataKey(encodedDataKey []byte) (string, string) {
	split := bytes.SplitN(encodedDataKey, nsKeySep, 2)
	return string(split[0][1:]), string(split[1])
}

func TimeIt(name string) func() {
	startTime := time.Now()
	return func() {
		logger.Infof("Time Taken to process %v : %v\n", name, time.Since(startTime))
	}
}

type DBValue struct {
	Key               []byte `json:"key"`
	BlockNumber       uint64 `json:"block_number"`
	TransactionNumber uint64 `json:"transaction_number"`
	DBMetadata        []byte `json:"db_metadata"`
	DBValue           []byte `json:"db_value"`
}

type Cache interface {
	Store(data map[statedb.CompositeKey]*version.Height)
	Load(key statedb.CompositeKey) (*version.Height, bool)
	Clear()
}
type cache struct {
	mutex sync.RWMutex
	data  map[statedb.CompositeKey]*version.Height
}

func newCache() Cache {
	return &cache{
		mutex: sync.RWMutex{},
		data:  make(map[statedb.CompositeKey]*version.Height),
	}
}

func (c *cache) Store(data map[statedb.CompositeKey]*version.Height) {
	c.mutex.Lock()
	c.data = data
	c.mutex.Unlock()
}

func (c *cache) Load(key statedb.CompositeKey) (*version.Height, bool) {
	c.mutex.RLock()
	height, ok := c.data[key]
	c.mutex.RUnlock()
	return height, ok
}

func (c *cache) Clear() {
	c.mutex.Lock()
	c.data = make(map[statedb.CompositeKey]*version.Height)
	c.mutex.Unlock()
}
