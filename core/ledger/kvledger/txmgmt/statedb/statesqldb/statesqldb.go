/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/


package statesqldb

import (
	"bytes"
	"strings"
	"sync"
	"time"

	"github.com/npci/drunix/common/flogging"
	keyvaluedatabase "github.com/npci/drunix/common/keyValueDatabase"
	"github.com/npci/drunix/common/metrics"
	"github.com/npci/drunix/core/ledger"
	"github.com/npci/drunix/core/ledger/internal/version"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/statedb"
)

var (
	logger                      = flogging.MustGetLogger("sqldb")
	sqlDbMetrics                *Metrics
	dataKeyPrefix               = []byte{'d'}
	nsKeySep                    = []byte{0x00}
	savePointKey                = "s"
	configEnvBytes              = string(encodeDataKey("", "CHANNEL_CONFIG_ENV_BYTES"))
	lastLifecycleBlockNumberKey = string(encodeDataKey("", "LAST_LIFECYCLE_BLOCK_NUMBER"))
)

func NewVersionedDBProvider(config *ledger.SqlDbConfig, metricsProvider metrics.Provider, sysNamespaces []string) (*VersionedDBProvider, error) {

	logger.Info("NewVersionedDBProvider : STATE SQL DB")

	sqlClient, err := newSqlClient(config)
	if err != nil {
		return nil, err
	}

	keyValueDBConn, err := keyvaluedatabase.GetKeyValueDBConnection()
	if err != nil {
		return nil, err
	}

	return &VersionedDBProvider{
		conf: config,
		client: &redisClient{
			Client:        keyValueDBConn.Client,
			ReplicaClient: keyValueDBConn.ReplicaClient,
		},
		sqlClient: sqlClient,
		databases: make(map[string]*versionedDB),
		metrics:   metricsProvider,
	}, nil
}

type VersionedDBProvider struct {
	conf      *ledger.SqlDbConfig
	client    *redisClient
	sqlClient SqlClient
	databases map[string]*versionedDB
	mux       sync.Mutex
	metrics   metrics.Provider
}

func (provider *VersionedDBProvider) GetDBHandle(dbName string, nsProvider statedb.NamespaceProvider) (statedb.VersionedDB, error) {
	provider.mux.Lock()
	defer provider.mux.Unlock()
	vdb := provider.databases[dbName]
	if vdb != nil {
		// DRUNIX : if the db alreay exists clear the cache
		vdb.sqlSchema.ClearCache()
		return vdb, nil
	}

	var err error
	vdb, err = provider.newVersionedDB(
		dbName,
		nsProvider,
		provider.metrics,
	)
	if err != nil {
		return nil, err
	}
	provider.databases[dbName] = vdb

	return vdb, nil
}

// To create newVersionedDB instance
func (provider *VersionedDBProvider) newVersionedDB(dbName string, nsProvider statedb.NamespaceProvider, metricsProvider metrics.Provider) (*versionedDB, error) {

	// DRUNIX TODO:
	// Remove lifecycle segregation based on peer ID.
	sqlSchema, err := provider.sqlClient.NewSchema(dbName, "peer", provider.conf.LitePeerEnabled)
	if err != nil {
		logger.Errorf("failed to initialize sql schema Error : %+v", err)
		return nil, err
	}

	/*
		DRUNIX
		Fixes the issue with multichannel, which registers the same metrics multiple times
		TODO :- segregate the matrics at channel level
	*/
	if metricsProvider != nil {
		registerMetricsOnce.Do(func() {
			sqlDbMetrics = NewMetrics(metricsProvider)
		})
	}

	return &versionedDB{
		redisSchema:     provider.client.NewSchema(dbName, provider.conf.PeerId),
		sqlSchema:       sqlSchema,
		chainName:       dbName,
		metrics:         sqlDbMetrics,
		versionCache:    newCache(),
		litePeerEnabled: provider.conf.LitePeerEnabled,
	}, nil
}
func (provider *VersionedDBProvider) ImportFromSnapshot(
	dbName string,
	savepoint *version.Height,
	itr statedb.FullScanIterator,
) error {

	db, err := provider.newVersionedDB(dbName, nil, nil)
	if err != nil {
		return err
	}

	data := make(map[string]*statedb.VersionedValue)
	for {
		versionedKV, err := itr.Next()
		if err != nil {
			return err
		}
		if versionedKV == nil {
			break
		}

		key := encodeDataKey(versionedKV.Namespace, versionedKV.Key)

		data[string(key)] = versionedKV.VersionedValue
	}

	data[savePointKey] = &statedb.VersionedValue{Value: savepoint.ToBytes(), Version: &version.Height{BlockNum: savepoint.BlockNum, TxNum: 0}}

	if err := db.sqlSchema.Set(data); err != nil {
		logger.Errorf("failed to set keys in sql : %v", err)
		return err
	}

	if err := db.redisSchema.SetHeight(savepoint.BlockNum); err != nil {
		logger.Errorf("failed to zset height : %+v", err)
		return err
	}

	return nil
}
func (provider *VersionedDBProvider) BytesKeySupported() bool {
	return false
}

/*
DRUNIX: This function is there to satisfy the interface method impl
*/
func (provider *VersionedDBProvider) Close() {}
func (provider *VersionedDBProvider) Drop(dbName string) error {
	return nil
}

type versionedDB struct {
	chainName       string // The name of the chain/channel.
	mux             sync.RWMutex
	redisSchema     RedisSchema
	sqlSchema       SqlSchema
	metrics         *Metrics
	versionCache    Cache
	litePeerEnabled bool
}

func (db *versionedDB) GetState(namespace string, key string) (*statedb.VersionedValue, error) {

	encodedDataKey := string(encodeDataKey(namespace, key))

	//TODO :- add metrices
	return db.sqlSchema.Get(encodedDataKey)
}
func (db *versionedDB) GetVersion(namespace string, key string) (*version.Height, error) {

	if height, found := db.GetCachedVersion(namespace, key); found {
		return height, nil
	}

	versionedValue, err := db.GetState(namespace, key)
	if err != nil {
		return nil, err
	}
	if versionedValue == nil {
		return nil, nil
	}
	return versionedValue.Version, nil
}

func (db *versionedDB) GetStateMultipleKeys(namespace string, keys []string) ([]*statedb.VersionedValue, error) {
	values := make([]*statedb.VersionedValue, len(keys))
	for index, key := range keys {
		value, err := db.GetState(namespace, key)
		if err != nil {
			return nil, err
		}
		values[index] = value
	}
	return values, nil
}

func (db *versionedDB) GetStateRangeScanIterator(namespace string, startKey string, endKey string) (statedb.ResultsIterator, error) {
	return db.GetStateRangeScanIteratorWithPagination(namespace, startKey, endKey, 10)
}
func (db *versionedDB) GetStateRangeScanIteratorWithPagination(namespace string, startKey string, endKey string, pageSize int32) (statedb.QueryResultsIterator, error) {

	if bytes.Equal([]byte(startKey), []byte{'\x01'}) {
		startKey = ""
	}

	encodedDataKey := string(encodeDataKey(namespace, startKey))

	return db.newSqlScanner(encodedDataKey, 0, int(pageSize), namespace, startKey), nil
}

func (db *versionedDB) ExecuteQuery(namespace, query string) (statedb.ResultsIterator, error) {
	return db.newSqlExecuter(namespace, query, "", 10), nil
}

func (db *versionedDB) ExecuteQueryWithPagination(namespace, query, bookmark string, pageSize int32) (statedb.QueryResultsIterator, error) {
	return db.newSqlExecuter(namespace, query, "", int(pageSize)), nil
}

func (db *versionedDB) ApplyUpdates(batch *statedb.UpdateBatch, height *version.Height) error {
	// DRUNIX : if litepeer enabled don't write to state database
	if db.litePeerEnabled {
		return nil
	}
	isLifecycleUpdate := false
	deleteData := []string{}
	updateData := make(map[string]*statedb.VersionedValue, 0)

	if len(batch.SchemaUpdates) != 0 {
		err := db.sqlSchema.Exec(batch.SchemaUpdates)
		if err != nil {
			logger.Errorf("Error in executing the schemas. err:%+v\n", err)
			return err
		}
	}

	namespaces := batch.GetUpdatedNamespaces()
	for _, ns := range namespaces {
		if db.IsLifecycleKeys(ns) {
			isLifecycleUpdate = true
		}
		updates := batch.GetUpdates(ns)
		for key, value := range updates {

			// DRUNIX TODO
			if db.IsCertKey(key) {
				keyvaluedatabase.CertCache.Store(key, value.Value)
				err := db.redisSchema.SetCert(key, value.Value)
				if err != nil {
					logger.Errorf("Error in setting cert. err: %v", err)
				}
			}

			encodedKey := string(encodeDataKey(ns, key))
			if value.Value != nil {
				updateData[encodedKey] = value
			} else {
				deleteData = append(deleteData, encodedKey)
			}
		}
	}

	errChan := make(chan error, 2)
	wg := sync.WaitGroup{}

	if len(updateData) > 0 {
		wg.Add(1)
		go func() {
			startTime := time.Now()
			err := db.sqlSchema.Set(updateData)
			if err != nil {
				db.metrics.DBCallSql.With("method_name", "ApplyUpdates", "call_type", "Set", "status", "fail").Add(1)
				db.metrics.DBCallTimeSQL.With("method_name", "ApplyUpdates", "call_type", "Set", "status", "fail").Observe(time.Since(startTime).Seconds())
				logger.Errorf("Error in batch sql write. err:%+v", err)
			}
			db.metrics.DBCallSql.With("method_name", "ApplyUpdates", "call_type", "Set", "status", "success").Add(1)
			db.metrics.DBCallTimeSQL.With("method_name", "ApplyUpdates", "call_type", "Set", "status", "success").Observe(time.Since(startTime).Seconds())
			errChan <- err
			wg.Done()
		}()
	}

	if len(deleteData) > 0 {
		wg.Add(1)
		go func() {
			startTime := time.Now()
			err := db.sqlSchema.Delete(deleteData)
			if err != nil {
				db.metrics.DBCallSql.With("method_name", "ApplyUpdates", "call_type", "Delete", "status", "fail").Add(1)
				db.metrics.DBCallTimeSQL.With("method_name", "ApplyUpdates", "call_type", "Delete", "status", "fail").Observe(time.Since(startTime).Seconds())
				logger.Errorf("Error in batch sql delete. err:%+v", err)
			}
			db.metrics.DBCallSql.With("method_name", "ApplyUpdates", "call_type", "Delete", "status", "success").Add(1)
			db.metrics.DBCallTimeSQL.With("method_name", "ApplyUpdates", "call_type", "Delete", "status", "success").Observe(time.Since(startTime).Seconds())
			errChan <- err
			wg.Done()
		}()
	}

	/*
		DRUNIX
		Added a waitgroup to wait to complete the sql writes before closing the error channel
	*/
	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return err
		}
	}

	if height != nil {
		/*
			DRUNIX
			To set the height in redis, to maintain the block height to purge orphan transactions
			only committing peers need to set height
		*/
		if !db.litePeerEnabled {
			if err := db.redisSchema.SetHeight(height.BlockNum); err != nil {
				logger.Errorf("failed to zset height : %+v", err)
				return err
			}
		}

		data := map[string]*statedb.VersionedValue{
			savePointKey: {Value: height.ToBytes(), Version: height},
		}

		if isLifecycleUpdate {
			data[lastLifecycleBlockNumberKey] = &statedb.VersionedValue{Value: height.ToBytes(), Version: height}
		}

		if err := db.sqlSchema.Set(data); err != nil {
			logger.Errorf("failed to set world state savepoint : %+v", err)
			return err
		}
	}
	return nil
}
func (db *versionedDB) GetLatestSavePoint() (*version.Height, error) {
	startTime := time.Now()
	versionedValue, err := db.sqlSchema.Get(savePointKey)
	if err != nil {
		db.metrics.DBCallSql.With("method_name", "GetLatestSavePoint", "call_type", "GetLifecycleKeys", "status", "fail").Add(1)
		db.metrics.DBCallTimeSQL.With("method_name", "GetLatestSavePoint", "call_type", "GetLifecycleKeys", "status", "fail").Observe(time.Since(startTime).Seconds())
		return nil, err
	}
	db.metrics.DBCallSql.With("method_name", "GetLatestSavePoint", "call_type", "GetLifecycleKeys", "status", "success").Add(1)
	db.metrics.DBCallTimeSQL.With("method_name", "GetLatestSavePoint", "call_type", "GetLifecycleKeys", "status", "success").Observe(time.Since(startTime).Seconds())
	if versionedValue == nil {
		return nil, nil
	}
	version, _, err := version.NewHeightFromBytes(versionedValue.Value)
	if err != nil {
		return nil, err
	}
	logger.Infof("GetLatestSavePoint BlockNo : %d, TxnNo : %d\n", version.BlockNum, version.TxNum)

	/*
		DRUNIX
		Since lite peers neither consume blocks nor commit to the ledger,
		they rely on the savepoint from the committing peer. This causes the statedb savepoint
		to always be ahead of the block store. To stabilize lite peers, align the savepoint with the block store.
	*/
	if db.litePeerEnabled {
		version.BlockNum = 0
		version.TxNum = 0
	}

	return version, nil
}

/*
DRUNIX: This function is there to satisfy the interface method impl
*/
func (db *versionedDB) ValidateKeyValue(key string, value []byte) error { return nil }

/*
DRUNIX: This function is there to satisfy the interface method impl
*/
func (db *versionedDB) BytesKeySupported() bool { return false }

/*
DRUNIX: This function is there to satisfy the interface method impl
*/
func (db *versionedDB) GetFullScanIterator(skipNamespace func(string) bool) (statedb.FullScanIterator, error) {
	return db.sqlSchema.NewFullDBScanner(skipNamespace)
}

/*
DRUNIX: This function is there to satisfy the interface method impl
*/
func (db *versionedDB) Open() error { return nil }

/*
DRUNIX: This function is there to satisfy the interface method impl
*/
func (db *versionedDB) Close() {}

func (db *versionedDB) IsLifecycleKeys(namespace string) bool {
	nss := strings.Split(namespace, "$$")
	if len(nss) > 0 {
		if nss[0] == "_lifecycle" || nss[0] == "" {
			return true
		}
	}
	return false
}

func (db *versionedDB) IsCertKey(key string) bool {
	return strings.Contains(key, "signcert_")
}

type SqlScanner struct {
	CompositeKey string
	Cursor       int
	PageSize     int
	Namespace    string
	Key          string
	db           *versionedDB
}

func (db *versionedDB) newSqlScanner(compositeKey string, cursor int, pageSize int, namespace string, key string) *SqlScanner {

	return &SqlScanner{
		CompositeKey: compositeKey,
		Cursor:       cursor,
		PageSize:     pageSize,
		Namespace:    namespace,
		Key:          key,
		db:           db,
	}
}

func (s *SqlScanner) Next() (*statedb.VersionedKV, error) {

	if s.Cursor >= s.PageSize {
		return nil, nil
	}

	encodedkey, versionedValue, err := s.db.sqlSchema.Next(s.CompositeKey, s.Cursor, s.PageSize)

	if err != nil {
		return nil, err
	}

	if versionedValue == nil {
		return nil, nil
	}

	_, key := decodeDataKey(encodedkey)

	s.Cursor += 1

	return &statedb.VersionedKV{
		CompositeKey: &statedb.CompositeKey{
			Namespace: s.Namespace,
			Key:       key,
		},
		VersionedValue: versionedValue,
	}, nil

}

func (s *SqlScanner) Close() {
}

func (s *SqlScanner) GetBookmarkAndClose() string {

	key, err := s.db.sqlSchema.NextKey(s.Key, s.Cursor, s.PageSize)
	if err != nil {
		logger.Errorf("GetBookmarkAndClose: error querying next key. key:%s, cursor:%d, error:%+v", s.Key, s.Cursor, err)
		return ""
	}

	return key
}

type SqlExecuter struct {
	Namespace string
	Query     string
	Bookmark  string
	PageSize  int
	Cursor    int
	db        *versionedDB
}

func (db *versionedDB) newSqlExecuter(namespace string, query string, bookmark string, pageSize int) *SqlExecuter {

	return &SqlExecuter{
		Namespace: namespace,
		Query:     query,
		Bookmark:  bookmark,
		PageSize:  pageSize,
		Cursor:    0,
		db:        db,
	}
}

func (s *SqlExecuter) Next() (*statedb.VersionedKV, error) {

	if s.Cursor >= s.PageSize {
		return nil, nil
	}

	keyBytes, versionedValue, err := s.db.sqlSchema.ExecuteNext(s.Namespace, s.Query, s.Cursor, s.PageSize)
	if err != nil {
		return nil, err
	}

	if versionedValue == nil {
		return nil, nil
	}

	_, key := decodeDataKey(keyBytes)

	s.Cursor += 1

	return &statedb.VersionedKV{
		CompositeKey: &statedb.CompositeKey{
			Namespace: s.Namespace,
			Key:       key,
		},
		VersionedValue: versionedValue,
	}, nil
}

func (s *SqlExecuter) Close() {
}

func (s *SqlExecuter) GetBookmarkAndClose() string {

	keyBytes, _, err := s.db.sqlSchema.ExecuteNext(s.Namespace, s.Query, s.Cursor, s.PageSize)
	if err != nil {
		logger.Errorf("err GetBookmarkAndClose : %v\n", err)
		return ""
	}

	_, key := decodeDataKey(keyBytes)

	return key
}

func (db *versionedDB) LoadCommittedVersions(keys []*statedb.CompositeKey) error {
	versions, err := db.sqlSchema.GetVersions(keys)
	if err != nil {
		return err
	}
	db.versionCache.Store(versions)
	return nil
}

func (db *versionedDB) GetCachedVersion(namespace, key string) (*version.Height, bool) {
	return db.versionCache.Load(statedb.CompositeKey{
		Namespace: namespace,
		Key:       key,
	})
}

func (db *versionedDB) ClearCachedVersions() {
	db.versionCache.Clear()
}
