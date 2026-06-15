/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/

package transientstore

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	proto "github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset"
	"github.com/hyperledger/fabric-protos-go/transientstore"
	keyvaluedatabase "github.com/npci/drunix/common/keyValueDatabase"
	"github.com/npci/drunix/common/metrics"
	"github.com/npci/drunix/common/util"
	"github.com/npci/drunix/core/ledger"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

/*
DRUNIX:
	This package initializes a new KVStore provider
	This KVStore provider is used only when enabled in environment var
	This KVStore provider implements StoreProvider interface and implements custom methods to retrieve transient data
*/

/*
	DRUNIX
	The key-value store batcher runs in a separate goroutine during channel initialization.
	Since lite peers are re-initialized on config updates, a new goroutine is created each time
	without closing the previous one, causing a memory leak. Added a check to start the batcher goroutine
	only if it does not already exist.
*/

var (
	storeMetricsCounter = metrics.CounterOpts{
		Namespace: "TransientStore",
		Subsystem: "DB",
		Name:      "no_of_calls",
		Help:      "Transient DB total number of calls made",
	}

	storeMetricsHist = metrics.HistogramOpts{
		Namespace:  "TransientStore",
		Subsystem:  "DB",
		Name:       "Duration",
		LabelNames: []string{"func", "Operation"},
		Help:       "Time taken for transientStore DB operations.",
	}

	registerMetricsOnce sync.Once
	/*
		DRUNIX: since this is a metrics object we won't be initializing it again so making it as global,
			   till we have channel level metrics decided
	*/
	transientDbCalls *TransientStoreMetrics
)

// store holds an instance of a levelDB.
type KVStore struct {
	ledgerID         string
	db               *keyvaluedatabase.KeyValueDBConnection
	transientMetrics *TransientStoreMetrics
	kvstorebatcher   KVStoreBatcher
}

type TransientStoreMetrics struct {
	DBCalls    metrics.Counter
	DBDuration metrics.Histogram
}

type kvStoreProvider struct {
	mux        sync.Mutex
	stores     map[string]*Store
	dbProvider *keyvaluedatabase.KeyValueDBConnection
	metrics    metrics.Provider
}

// NewStoreProvider instantiates TransientStoreProvider
func NewKVStoreProvider(metrics metrics.Provider) (StoreProvider, error) {

	provider, err := newKVStoreProvider()
	if err != nil {
		return nil, errors.WithMessagef(err, "could not construct KV storage provider")
	}

	provider.metrics = metrics

	return provider, nil
}

// Private method used to unwind a dependency between the package level Drop and NewStoreProvider routines.
// This routine must be invoked while holding the newStoreProvider file lock.
func newKVStoreProvider() (*kvStoreProvider, error) {
	logger.Debugw("opening KV store provider")

	kvDBProvider, err := keyvaluedatabase.GetKeyValueDBConnection()
	if err != nil {
		return nil, errors.WithMessage(err, "could not open key-value store")
	}

	logger.Info("the key-value store connection established")
	provider := &kvStoreProvider{dbProvider: kvDBProvider, stores: make(map[string]*Store)}

	return provider, nil
}

// OpenStore returns a handle to a ledgerId in Store
func (provider *kvStoreProvider) OpenStore(ledgerID string) (*Store, error) {

	provider.mux.Lock()
	defer provider.mux.Unlock()
	store := provider.stores[ledgerID]
	if store != nil {
		return store, nil
	}

	registerMetricsOnce.Do(func() {
		transientDbCalls = &TransientStoreMetrics{
			DBCalls:    provider.metrics.NewCounter(storeMetricsCounter),
			DBDuration: provider.metrics.NewHistogram(storeMetricsHist),
		}
	})

	store = &Store{KVStore: &KVStore{db: provider.dbProvider, transientMetrics: transientDbCalls, ledgerID: ledgerID}}

	store.KVStore.kvstorebatcher = newKVStoreBatcher(store)

	provider.stores[ledgerID] = store

	return store, nil
}

// Close closes the TransientStoreProvider
func (provider *kvStoreProvider) Close() {

	if provider.dbProvider != nil {
		if err := provider.dbProvider.Client.Close(); err != nil {
			logger.Errorf("failed to close the kvstore provider connection : %+v", err)
		}
	}
}

/*
DRUNIX :

	"GetTxPvtRWSetByTxidV2": retrives the private data from transient DB.
*/
func (s *KVStore) GetTxPvtRWSetByTxidV2(rwsetKeysMap map[string][]ledger.PvtNsCollFilter) (map[string]map[string][]*EndorserPvtSimulationResults, error) {

	scanKeys := make(map[string]string, len(rwsetKeysMap))
	index := 0
	for txId := range rwsetKeysMap {
		scanKeys[txId] = string(createTxidRangeStartKey(txId))
		index++
	}

	st := time.Now()
	txnKeyValueMap, err := s.PrivateDataHGetAllBatch(scanKeys)
	if err != nil {
		return nil, err
	}

	s.DbCallDurationMetric(time.Since(st), []string{"func", "GetTxPvtRWSetByTxidV2", "Operation", "PrivateDataHGetAllBatch"})
	s.AddDbCallsMetrics(1)

	results := make(map[string]map[string][]*EndorserPvtSimulationResults, 0)

	for txn, keyValueMap := range txnKeyValueMap {
		results[txn] = make(map[string][]*EndorserPvtSimulationResults)
		for key, value := range keyValueMap {
			if !strings.Contains(key, scanKeys[txn]) {
				continue
			}

			if value == "" {
				results[txn][key] = nil
				continue
			}

			for _, filter := range rwsetKeysMap[txn] {
				result, err := s.ProcessPrivateData([]byte(key), []byte(value), filter)
				if err != nil {
					return nil, err
				}
				if len(results[txn][key]) == 0 {
					results[txn][key] = []*EndorserPvtSimulationResults{result}
				} else {
					results[txn][key] = append(results[txn][key], result)
				}
			}
		}
	}
	return results, nil
}

func (s *KVStore) ProcessPrivateData(dbKey []byte, dbVal []byte, filter ledger.PvtNsCollFilter) (*EndorserPvtSimulationResults, error) {

	_, blockHeight, err := splitCompositeKeyOfPvtRWSet(dbKey)
	if err != nil {
		return nil, err
	}

	txPvtRWSet := &rwset.TxPvtReadWriteSet{}
	txPvtRWSetWithConfig := &transientstore.TxPvtReadWriteSetWithConfigInfo{}

	var filteredTxPvtRWSet *rwset.TxPvtReadWriteSet
	if dbVal[0] == nilByte {
		// new proto, i.e., TxPvtReadWriteSetWithConfigInfo
		if err := proto.Unmarshal(dbVal[1:], txPvtRWSetWithConfig); err != nil {
			return nil, err
		}

		// trim the tx rwset based on the current collection filter,
		// nil will be returned to filteredTxPvtRWSet if the transient store txid entry does not contain the data for the collection
		filteredTxPvtRWSet = trimPvtWSet(txPvtRWSetWithConfig.GetPvtRwset(), filter)
		configs, err := trimPvtCollectionConfigs(txPvtRWSetWithConfig.CollectionConfigs, filter)
		if err != nil {
			return nil, err
		}
		txPvtRWSetWithConfig.CollectionConfigs = configs
	} else {
		// old proto, i.e., TxPvtReadWriteSet
		if err := proto.Unmarshal(dbVal, txPvtRWSet); err != nil {
			return nil, err
		}
		filteredTxPvtRWSet = trimPvtWSet(txPvtRWSet, filter)
	}

	txPvtRWSetWithConfig.PvtRwset = filteredTxPvtRWSet

	return &EndorserPvtSimulationResults{
		ReceivedAtBlockHeight:          blockHeight,
		PvtSimulationResultsWithConfig: txPvtRWSetWithConfig,
	}, nil
}

/*
DRUNIX :

	"PurgeTxIdsByHeight":  purge all the transactions pvt data  of older blocks from the transient store..
*/
func (s *KVStore) PurgeTxIdsByHeight(height uint64) error {
	defer func(startTime time.Time) {
		s.DbCallDurationMetric(time.Since(startTime), []string{"func", "PurgeTxIdsByHeight", "Operation", "Delete"})
	}(time.Now())

	s.AddDbCallsMetrics(1)

	return s.PurgeBelowHeight(height)
}

/*
DRUNIX :

	"AddDbCallsMetrics":  added metrics to record #of db calls made to transient store
*/
func (s *KVStore) AddDbCallsMetrics(f float64) {
	s.transientMetrics.DBCalls.Add(f)
}

/*
DRUNIX :

	"DbCallDurationMetric":  added metrics to record the duration of each DB call made to transient store
*/
func (s *KVStore) DbCallDurationMetric(t time.Duration, lables []string) {
	s.transientMetrics.DBDuration.With(lables...).Observe(t.Seconds())
}

// Persist stores the private write set of a transaction along with the collection config
// in the transient store based on txid and the block height the private data was received at
func (s *KVStore) Persist(txid string, blockHeight uint64,
	privateSimulationResultsWithConfig *transientstore.TxPvtReadWriteSetWithConfigInfo) error {
	logger.Debugf("Persisting private data to KV transient store for txid [%s] at block height [%d]", txid, blockHeight)

	// Create compositeKey with appropriate prefix, txid, uuid and blockHeight
	// Due to the fact that the txid may have multiple private write sets persisted from different
	// endorsers (via Gossip), we postfix an uuid with the txid to avoid collision.
	uuid := util.GenerateUUID()
	compositeKeyPvtRWSet := createCompositeKeyForPvtRWSet(txid, uuid, blockHeight)
	privateSimulationResultsWithConfigBytes, err := proto.Marshal(privateSimulationResultsWithConfig)
	if err != nil {
		return err
	}

	// Note that some rwset.TxPvtReadWriteSet may exist in the transient store immediately after
	// upgrading the peer to v1.2. In order to differentiate between new proto and old proto while
	// retrieving, a nil byte is prepended to the new proto, i.e., privateSimulationResultsWithConfigBytes,
	// as a marshaled message can never start with a nil byte. In v1.3, we can avoid prepending the
	// nil byte.
	value := append([]byte{nilByte}, privateSimulationResultsWithConfigBytes...)

	// Create two index: (i) by txid, and (ii) by height

	// Create compositeKey for purge index by height with appropriate prefix, blockHeight,
	// txid, uuid and store the compositeKey (purge index) with a nil byte as value. Note that
	// the purge index is used to remove orphan entries in the transient store (which are not removed
	// by PurgeTxids()) using BTL policy by PurgeBelowHeight(). Note that orphan entries are due to transaction
	// that gets endorsed but not submitted by the client for commit)
	compositeKeyPurgeIndexByHeight := createCompositeKeyForPurgeIndexByHeight(blockHeight, txid, uuid)

	// Create compositeKey for purge index by txid with appropriate prefix, txid, uuid,
	// blockHeight and store the compositeKey (purge index) with a nil byte as value.
	// Though compositeKeyPvtRWSet itself can be used to purge private write set by txid,
	// we create a separate composite key with a nil byte as value. The reason is that
	// if we use compositeKeyPvtRWSet, we unnecessarily read (potentially large) private write
	// set associated with the key from db. Note that this purge index is used to remove non-orphan
	// entries in the transient store and is used by PurgeTxids()
	// Note: We can create compositeKeyPurgeIndexByTxid by just replacing the prefix of compositeKeyPvtRWSet
	// with purgeIndexByTxidPrefix. For code readability and to be expressive, we use a
	// createCompositeKeyForPurgeIndexByTxid() instead.
	compositeKeyPurgeIndexByTxid := createCompositeKeyForPurgeIndexByTxid(txid, uuid, blockHeight)

	/*
		DRUNIX :
		if external kv store is enabled to be used as a transient store :
			- insert transient data in external kv store
			- else store in leveldb
			- if leveldb or external kv store is not opened then the CP panics

		the map in the external kv store is initialised with default size (8 buckets * 8 k-v pairs), which is overkill.
		we are not setting the ttl as well. we are purging by height. so ttl map is not required.
	*/

	kvdbBatch := map[string]string{
		string(compositeKeyPvtRWSet):           string(value),
		string(compositeKeyPurgeIndexByHeight): string(emptyValue),
		string(compositeKeyPurgeIndexByTxid):   string(emptyValue),
	}

	return s.kvstorebatcher.Set(txid, kvdbBatch)

}

func (s *KVStore) PrivateDataHGetAllBatch(keys map[string]string) (map[string]map[string]string, error) {
	result := make(map[string]map[string]string, len(keys))
	pipe := s.db.ReplicaClient.Pipeline()
	ctx := context.Background()
	cmds := make(map[string]*redis.MapStringStringCmd, len(keys))

	for idx := range keys {
		cmds[idx] = pipe.HGetAll(ctx, idx)
	}
	_, err := pipe.Exec(ctx)
	if err != nil && err == redis.Nil {
		logger.Errorf("error in retreiving PrivateDataHGetAllBatch value bytes : %+v", err)
	} else if err != nil {
		logger.Errorf("error executing PrivateDataHGetAllBatch pipeline :%v", err)
		return nil, err
	}

	for idx, cmd := range cmds {
		if err := cmd.Err(); err != nil && err == redis.Nil {
			logger.Error("PrivateDataHGetAllBatch key-value store Nil for the key:", cmd.Args())
		} else {
			res, _ := cmd.Result()
			result[idx] = res
		}

	}
	return result, nil
}

/*
DRUNIX
To purge the pvt data based on the ledger height after the pvt data max block retention
It will be purged by the CP with lowest block height
This will cause an edge case if a CP was part of the network and later removed,
the minHeight of the CP will be of the removed and txns won't be purged.
this can be handled by removing the height of the removed peerId from transient store
*/
func (s *KVStore) PurgeBelowHeight(thresholdHeight uint64) error {

	/*
		DRUNIX
		Fetching the min height and peerId of the existing CPs,
		and if itself is of minHeight, by comparing the minPeerId and self peerId,
		it will purge the txns below height, else it will skip the purge,
		so that the txns won't be purged by the leading peer,
		and lagging peer won't have to populate pvtData from remote peer
	*/
	minPeerId, _, err := s.GetMinHeight()
	if err != nil {
		return err
	}
	if minPeerId != s.db.PeerId {
		return nil
	}

	ctx := context.Background()

	logger.Infof("Purging pvtData below threshold height %v\n", thresholdHeight)

	// Find all txnIDs with blockHeight < thresholdHeight
	txnIDs, err := s.db.Client.ZRangeByScore(ctx, fmt.Sprintf("%s:txns_by_block", s.ledgerID), &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%d", thresholdHeight-1),
	}).Result()
	if err != nil {
		return err
	}

	// Delete all txn hashes and remove from sorted set
	if len(txnIDs) > 0 {
		// Use pipeline for efficiency
		pipe := s.db.Client.Pipeline()
		err := pipe.Del(ctx, txnIDs...).Err()
		if err != nil {
			return err
		}
		err = pipe.ZRem(ctx, fmt.Sprintf("%s:txns_by_block", s.ledgerID), txnIDs).Err()
		if err != nil {
			return err
		}
		_, err = pipe.Exec(ctx)
		return err
	}

	return nil
}

func (s *KVStore) PurgeByTxids(txIds []string) error {

	/*
		DRUNIX
		Fetching the min height and peerId of the existing CPs,
		and if itself is of minHeight, by comparing the minPeerId and self peerId,
		it will purge the pvtData with txnIds, else it will skip the purge,
		so that the txns won't be purged by the leading peer,
		and lagging peer won't have to populate pvtData from remote peer
	*/

	defer func(startTime time.Time) {
		s.DbCallDurationMetric(time.Since(startTime), []string{"func", "PurgeTxIdsByHeight", "Operation", "Delete"})
	}(time.Now())

	s.AddDbCallsMetrics(1)

	minPeerId, _, err := s.GetMinHeight()
	if err != nil {
		return err
	}
	if minPeerId != s.db.PeerId {
		return nil
	}

	ctx := context.Background()

	logger.Infof("Purging pvtData by TxIds\n")

	// Delete all txn hashes and remove from sorted set
	if len(txIds) > 0 {
		// Use pipeline for efficiency
		pipe := s.db.Client.Pipeline()
		err := pipe.Del(ctx, txIds...).Err()
		if err != nil {
			return err
		}
		err = pipe.ZRem(ctx, fmt.Sprintf("%s:txns_by_block", s.ledgerID), txIds).Err()
		if err != nil {
			return err
		}
		_, err = pipe.Exec(ctx)
		return err
	}

	return nil
}

/*
DRUNIX
Fetch the min block height of the existing CPs
*/
func (s *KVStore) GetMinHeight() (string, float64, error) {
	res, err := s.db.Client.ZRangeWithScores(context.Background(), fmt.Sprintf("%s:heights", s.ledgerID), 0, 0).Result()
	if err != nil || len(res) == 0 {
		return "", 0, fmt.Errorf("failed get min height : [%v]", err)
	}

	peerId, ok := res[0].Member.(string)
	if !ok {
		return "", 0, errors.New("ZRangeWithScores : invalid peerId")
	}

	return peerId, res[0].Score, nil
}
