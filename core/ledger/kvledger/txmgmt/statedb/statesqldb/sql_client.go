/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/

package statesqldb

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"

	"gorm.io/datatypes"
	gormLogger "gorm.io/gorm/logger"

	"github.com/npci/drunix/core/ledger"
	"github.com/npci/drunix/core/ledger/internal/version"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/statedb"

	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type sqlClient struct {
	Client    *gorm.DB
	Batchsize int
}

type sqlSchema struct {
	*sqlClient
	schema           string
	tableColumnCache map[string][]string
	cache            sync.Map
	peerid           string
	litePeerEnabled  bool
	SqlBatcher       SqlBatcher
	tableCache       TableCache
}

type SqlClient interface {
	NewSchema(schema string, peerid string, litePeerEnabled bool) (SqlSchema, error)
}

type SqlSchema interface {
	Get(key string) (*statedb.VersionedValue, error)
	Scan(keyPrefix string, cursor uint64, count int64) (*sql.Rows, error)
	Set(batch map[string]*statedb.VersionedValue) error
	Delete(key []string) error
	Close() error
	Next(key string, cursor int, pagesize int) ([]byte, *statedb.VersionedValue, error)
	NextKey(key string, cursor int, pagesize int) (string, error)
	ExecuteNext(namespace string, query string, cursor int, pageSize int) ([]byte, *statedb.VersionedValue, error)
	Exec([]string) error
	GetVersions(keys []*statedb.CompositeKey) (map[statedb.CompositeKey]*version.Height, error)
	NewFullDBScanner(skipNamespace func(namespace string) bool) (*fullDBScanner, error)
	// DRUNIX : to clear the cache. Whenever GetDBHandle of version provider is called clear the cache.
	ClearCache()
}

func (sql *sqlClient) NewSchema(schema string, peerid string, litePeerEnabled bool) (SqlSchema, error) {

	migration := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s";`, schema)

	err := sql.Client.Exec(migration).Error
	if err != nil {
		return nil, fmt.Errorf("error while adding the schema: %v", err)
	}

	lifecycle := &Lifecycle{channel: schema, peerId: peerid}
	err = sql.Client.Table(lifecycle.TableName()).AutoMigrate(lifecycle)
	if err != nil {
		return nil, fmt.Errorf("error while adding cp lifecycle schema:%v", err)
	}

	cachedTables, err := newTableCache(schema, sql.Client)
	if err != nil {
		return nil, fmt.Errorf("failed to initiate table cache: %v", err)
	}

	sqlschema := &sqlSchema{
		sqlClient:        sql,
		schema:           schema,
		tableColumnCache: make(map[string][]string),
		cache:            sync.Map{},
		peerid:           strings.ToLower(strings.ReplaceAll(peerid, ".", "_")),
		litePeerEnabled:  litePeerEnabled,
		tableCache:       cachedTables,
	}

	sqlschema.SqlBatcher = NewSqlBatcher(sqlschema)

	return sqlschema, nil
}

func (sql *sqlSchema) Exec(schemas []string) error {
	for _, schema := range schemas {
		tx := sql.sqlClient.Client.Begin()
		if tx.Error != nil {
			return fmt.Errorf("failed to begin transaction: %w", tx.Error)
		}

		// Ensure rollback on panic or early return
		committed := false
		defer func() {
			if !committed {
				_ = tx.Rollback()
			}
		}()

		statements := strings.Split(schema, ";")
		for _, stmt := range statements {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}

			if err := tx.Exec(stmt).Error; err != nil {
				return fmt.Errorf("error executing statement [%s]: %w", stmt, err)
			}
		}

		if err := tx.Commit().Error; err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}

		committed = true
	}

	return nil
}

func (sql *sqlSchema) Get(key string) (*statedb.VersionedValue, error) {

	// defer TimeIt("SQL GET")()

	var err error
	var data DBValue

	table, cacheEnabled, err := sql.GetTable(key)
	if err != nil {
		return nil, err
	}

	if cacheEnabled {
		if versionedValue, ok := sql.cache.Load(key); ok {
			return versionedValue.(*statedb.VersionedValue), nil
		}
		logger.Infof("Key %s not found in cache", key)
	}
	// data, err = sql.SqlBatcher.Get(key)
	err = sql.Client.Table(table).Select("key", "block_number", "transaction_number", "db_metadata", "db_value").Where("key = ?", []byte(key)).Limit(1).Scan(&data).Error

	if err == gorm.ErrRecordNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if data.Key == nil {
		return nil, nil
	}

	versionedValue := &statedb.VersionedValue{
		Version: &version.Height{
			BlockNum: data.BlockNumber,
			TxNum:    data.TransactionNumber,
		},
		Value:    data.DBValue,
		Metadata: data.DBMetadata,
	}

	if cacheEnabled {
		sql.cache.Store(key, versionedValue)
		logger.Infof("Key %s stored in cache", key)
	}

	return versionedValue, nil
}

func (sql *sqlSchema) Scan(key string, cursor uint64, count int64) (*sql.Rows, error) {

	return nil, nil
}

func (sql *sqlSchema) Set(data map[string]*statedb.VersionedValue) error {

	// defer TimeIt("SQL SET")()

	dataToBeCached := make(map[string]*statedb.VersionedValue)
	parsedData := make(map[string][]map[string]interface{})

	for key, value := range data {

		table, cacheEnabled, err := sql.GetTable(key)
		if err != nil {
			return err
		}
		if cacheEnabled {
			dataToBeCached[key] = value
		}

		data := make(map[string]interface{})
		data["db_value"] = value.Value
		if !strings.HasSuffix(table, "_lifecycle") && !strings.Contains(table, "$$h") {
			err := json.Unmarshal(value.Value, &data)
			if err != nil {
				// logger.Warningf("failed to unmarshal data, %v", err)
			}
			data["db_value"] = datatypes.JSON(value.Value)
		}
		data["key"] = []byte(key)
		data["db_metadata"] = value.Metadata
		data["block_number"] = value.Version.BlockNum
		data["transaction_number"] = value.Version.TxNum

		data, err = sql.filterKeys(table, data)
		if err != nil {
			return err
		}

		parsedData[table] = append(parsedData[table], data)
	}

	tableBatchedData := make(map[string][][]map[string]interface{})
	var workerCount int
	for table, data := range parsedData {
		dataLen := len(data)
		for i := 0; i < dataLen; i += sql.Batchsize {
			end := i + sql.Batchsize
			if end > dataLen {
				end = dataLen
			}
			batch := data[i:end]
			tableBatchedData[table] = append(tableBatchedData[table], batch)
			workerCount++
		}
	}

	errorCh := make(chan error, workerCount)
	var wg sync.WaitGroup

	for table, batch := range tableBatchedData {
		keys := []string{}
		for key := range batch[0][0] {
			keys = append(keys, key)
		}
		for _, data := range batch {
			wg.Add(1)
			go func(table string, data []map[string]interface{}, keys []string) {
				err := sql.Client.Clauses(clause.OnConflict{UpdateAll: true, DoUpdates: clause.AssignmentColumns(keys), Columns: []clause.Column{{Name: "key"}}}).Table(table).Create(&data).Error
				if err != nil {
					logger.Errorf("SQL SET err: %v\n", err)
				}
				errorCh <- err
				wg.Done()
			}(table, data, keys)
		}
	}

	wg.Wait()
	close(errorCh)

	for i := 0; i < workerCount; i++ {
		err := <-errorCh
		if err != nil {
			return err
		}
	}

	for key, value := range dataToBeCached {
		sql.cache.Store(key, value)
		logger.Infof("Key %s stored in cache", key)
	}

	return nil
}

func (sql *sqlSchema) Delete(keys []string) error {

	// defer TimeIt("SQL DELETE")()

	if len(keys) == 0 {
		return nil
	}

	tableDataMap := make(map[string][][]byte)
	for _, key := range keys {
		table, _, err := sql.GetTable(key)
		if err != nil {
			return err
		}
		tableDataMap[table] = append(tableDataMap[table], []byte(key))

	}

	tableBatchedDataMap := make(map[string][][][]byte)
	var workerCount int
	for table, data := range tableDataMap {
		dataLen := len(data)
		for i := 0; i < dataLen; i += sql.Batchsize {
			end := i + sql.Batchsize
			if end > dataLen {
				end = dataLen
			}
			batch := data[i:end]
			tableBatchedDataMap[table] = append(tableBatchedDataMap[table], batch)
			workerCount++
		}
	}

	errorCh := make(chan error, workerCount)
	var wg sync.WaitGroup

	for table, batch := range tableBatchedDataMap {
		for _, data := range batch {
			wg.Add(1)
			go func(table string, data [][]byte) {
				err := sql.Client.Table(table).Where("key IN ?", data).Delete(nil).Error
				if err != nil {
					logger.Errorf("SQL DELETE err: %v\n", err)
				}
				errorCh <- err
				wg.Done()
			}(table, data)
		}
	}

	/*
		DRUNIX
		Added a waitgroup to wait to complete the sql writes & close the error channel
	*/
	wg.Wait()
	close(errorCh)

	for i := 0; i < workerCount; i++ {
		err := <-errorCh
		if err != nil {
			return err
		}
	}

	return nil
}

func newSqlClient(config *ledger.SqlDbConfig) (SqlClient, error) {

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		config.Host, config.Port, config.User, config.Password, config.DBName, config.Sslmode)

	newLogger := gormLogger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		gormLogger.Config{
			SlowThreshold:             1 * time.Second, // Slow SQL threshold
			LogLevel:                  gormLogger.Error,
			IgnoreRecordNotFoundError: true, // Ignore ErrRecordNotFound error for logger
			Colorful:                  true,
		},
	)

	client, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: newLogger, PrepareStmt: config.PrepareStatment, CreateBatchSize: config.Batchsize})
	if err != nil {
		return nil, err
	}

	sqlDb, err := client.DB()
	if err != nil {
		return nil, err
	}

	if config.MaxIdleConns != 0 {
		sqlDb.SetMaxIdleConns(config.MaxIdleConns)
	}
	if config.MaxOpenConns != 0 {
		sqlDb.SetMaxOpenConns(config.MaxOpenConns)
	}
	if config.ConnMaxLifetime != 0 {
		sqlDb.SetConnMaxLifetime(config.ConnMaxLifetime)
	}

	err = sqlDb.Ping()
	if err != nil {
		return nil, err
	}

	return &sqlClient{
		Client:    client,
		Batchsize: config.Batchsize,
	}, nil
}

func (sql *sqlSchema) Close() error {

	sqlDb, err := sql.Client.DB()
	if err != nil {
		return err
	}
	err = sqlDb.Close()
	if err != nil {
		return err
	}
	return nil
}

func (sql *sqlSchema) Next(key string, cursor int, pagesize int) ([]byte, *statedb.VersionedValue, error) {

	table, _, err := sql.GetTable(key)
	if err != nil {
		return nil, nil, err
	}

	var data DBValue
	err = sql.Client.Table(table).Select("key", "block_number", "transaction_number", "db_metadata", "db_value").Limit(1).Offset(cursor).Where("encode(key, 'escape') ILIKE ?", fmt.Sprintf("%%%v%%", strings.ReplaceAll(key, "\000", "\\\\000"))).Find(&data).Error
	if err != nil {
		return nil, nil, err
	}

	if data.Key == nil {
		return nil, nil, nil
	}

	versionedValue := &statedb.VersionedValue{
		Version: &version.Height{
			BlockNum: data.BlockNumber,
			TxNum:    data.TransactionNumber,
		},
		Value:    data.DBValue,
		Metadata: data.DBMetadata,
	}

	return data.Key, versionedValue, nil
}

func (sql *sqlSchema) NextKey(key string, cursor int, pagesize int) (string, error) {

	table, _, err := sql.GetTable(key)
	if err != nil {
		return "", err
	}

	var data map[string]interface{}

	err = sql.Client.Table(table).Limit(pagesize).Offset(cursor).Where("encode(key, 'escape') ILIKE ?", fmt.Sprintf("%%%v%%", strings.ReplaceAll(key, "\000", "\\\\000"))).Find(&data).Error
	if err != nil {
		return "", err
	}

	return string(data["key"].([]byte)), nil
}

func (sql *sqlSchema) GetTable(key string) (string, bool, error) {

	if lastLifecycleBlockNumberKey == key {
		return fmt.Sprintf("%s.%s_lifecycle", sql.schema, sql.peerid), false, nil
	}

	if strings.HasPrefix(key, "d_lifecycle") || strings.HasPrefix(key, "dlscc") || slices.Contains([]string{configEnvBytes, savePointKey, lastLifecycleBlockNumberKey}, key) {
		return fmt.Sprintf("%s.%s_lifecycle", sql.schema, sql.peerid), true, nil
	}

	keyWithoutPrefix, _ := bytes.CutPrefix([]byte(key), dataKeyPrefix)

	keyBytes := bytes.SplitN(keyWithoutPrefix, nsKeySep, 2)

	err := sql.tableCache.CreateIfNotExists(string(keyBytes[0]))
	if err != nil {
		return "", false, err
	}

	return fmt.Sprintf("%s.%s", sql.schema, string(keyBytes[0])), false, nil
}

func (sql *sqlSchema) filterKeys(table string, data map[string]interface{}) (map[string]interface{}, error) {

	var columns []string

	tableSplit := strings.Split(table, ".")
	if len(tableSplit) != 2 {
		return nil, errors.New("invalid table name, schema not found in table name")
	}
	tableName := tableSplit[1]

	if _, ok := sql.tableColumnCache[table]; !ok {
		err := sql.Client.Raw(fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE table_schema = '%v' AND table_name = '%v'", sql.schema, tableName)).Scan(&columns).Error
		if err != nil {
			return nil, err
		}
		sql.tableColumnCache[table] = columns
	} else {
		columns = sql.tableColumnCache[table]
	}

	validColumns := make(map[string]struct{})
	for _, column := range columns {
		validColumns[column] = struct{}{}
	}

	filteredData := make(map[string]interface{})
	for key, value := range data {
		if _, exists := validColumns[key]; exists {
			filteredData[key] = value
		}
	}

	return filteredData, nil
}

/*
	DRUNIX:-
	Added implementation to convert rich queries in couchdb format to sql format
*/
// GetQueryResult
func (sql *sqlSchema) ExecuteNext(namespace string, query string, cursor int, pageSize int) ([]byte, *statedb.VersionedValue, error) {

	// defer TimeIt("SQL EXECUTENEXT")()

	type richQueryStruct struct {
		Selector map[string]interface{} `json:"selector"`
	}
	richQuery := richQueryStruct{}
	err := json.Unmarshal([]byte(query), &richQuery)
	if err != nil {
		return nil, nil, err
	}

	andQueryKeys := []string{}
	orQueryKeys := []string{}
	queryValues := []interface{}{}
	for key, value := range richQuery.Selector {
		if key == "$and" {
			andQueries, ok := value.([]interface{})
			if !ok {
				return nil, nil, errors.New("invalid '$and' query ([]interface{})")
			}
			for _, andQuery := range andQueries {
				mapQuery, ok := andQuery.(map[string]interface{})
				if !ok {
					return nil, nil, errors.New("invalid '$and' query map[string]interface{}")
				}
				for key, value := range mapQuery {
					andQueryKeys = append(andQueryKeys, fmt.Sprintf("db_value->>'%s' = ?", key))
					queryValues = append(queryValues, value)
				}
			}
			continue
		}
		if key == "$or" {
			orQueries, ok := value.([]interface{})
			if !ok {
				return nil, nil, errors.New("invalid '$or' query ([]interface{})")
			}
			for _, orQuery := range orQueries {
				mapQuery, ok := orQuery.(map[string]interface{})
				if !ok {
					return nil, nil, errors.New("invalid '$or' query map[string]interface{}")
				}
				for key, value := range mapQuery {
					orQueryKeys = append(orQueryKeys, fmt.Sprintf("db_value->>'%s' = ?", key))
					queryValues = append(queryValues, value)
				}
			}
			continue
		}
		andQueryKeys = append(andQueryKeys, fmt.Sprintf("db_value->>'%s' = ?", key))
		queryValues = append(queryValues, value)
	}

	joinedQuery := strings.Join(andQueryKeys, " AND ")
	joinedQuery = strings.Join(append([]string{joinedQuery}, orQueryKeys...), " OR ")

	table, _, err := sql.GetTable(namespace)
	if err != nil {
		return nil, nil, err
	}

	var data DBValue
	err = sql.Client.Table(table).Select("key", "block_number", "transaction_number", "db_metadata", "db_value").Where(joinedQuery, queryValues...).Limit(1).Offset(cursor).Find(&data).Error

	if err != nil {
		return nil, nil, err
	}

	if data.Key == nil {
		return nil, nil, nil
	}

	versionedValue := &statedb.VersionedValue{
		Version: &version.Height{
			BlockNum: data.BlockNumber,
			TxNum:    data.TransactionNumber,
		},
		Value:    data.DBValue,
		Metadata: data.DBMetadata,
	}

	return data.Key, versionedValue, nil
}

/*
func (sql *sqlSchema) ExecuteNext(namespace string, query string, cursor int, pageSize int) ([]byte, []byte, error) {

	// fmt.Println("---------------------\n---------------------YBDB EXECUTENEXT\n---------------------\n---------------------")

	// fmt.Println(namespace, query, cursor, pageSize)

	fmt.Println("GetQueryResult called in ExecuteNext")

	table := GetTable(namespace)

	var data map[string]interface{}

	// err := sql.Client.Raw(fmt.Sprintf(
	// 	`WITH json_data AS (
	// 		SELECT *, convert_from(value, 'UTF8')::jsonb AS jsonb_data
	// 		FROM %v
	// 	  ) %s LIMIT %d OFFSET %d;
	// 	`, table, query, pageSize, cursor)).Scan(&data).Error

	err := sql.Client.Table(table).Limit(1).Offset(cursor).Where(query).Find(&data).Error

	if err != nil {
		return nil, nil, err
	}

	if data == nil {
		return nil, nil, nil
	}

	dbValue := DBValue{}
	dbValue.Version, _ = data["db_value_version"].([]byte)
	dbValue.Value, _ = data["db_value"].([]byte)
	dbValue.Metadata, _ = data["db_metadata"].([]byte)
	key, _ := data["key"].([]byte)
	dbValueBytes, err := proto.Marshal(&dbValue)
	if err != nil {
		return nil, nil, err
	}

	return key, dbValueBytes, nil
}*/

func (sql *sqlSchema) GetTableFromNamespace(namespace string) (string, error) {

	if strings.HasPrefix(namespace, "_lifecycle") || namespace == "lscc" {
		return fmt.Sprintf("%s.%s_lifecycle", sql.schema, sql.peerid), nil
	}

	err := sql.tableCache.CreateIfNotExists(namespace)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s.%s", sql.schema, namespace), nil
}

func (sql *sqlSchema) GetVersions(keys []*statedb.CompositeKey) (map[statedb.CompositeKey]*version.Height, error) {

	versionsMap := make(map[statedb.CompositeKey]*version.Height, len(keys))

	tableDataMap := make(map[string][][]byte)
	for _, key := range keys {
		versionsMap[statedb.CompositeKey{
			Namespace: key.Namespace,
			Key:       key.Key,
		}] = nil
		keyByte := encodeDataKey(key.Namespace, key.Key)
		table, err := sql.GetTableFromNamespace(key.Namespace)
		if err != nil {
			return nil, err
		}
		tableDataMap[table] = append(tableDataMap[table], keyByte)
	}

	tableBatchedDataMap := make(map[string][][][]byte)
	var workerCount int
	for table, data := range tableDataMap {
		dataLen := len(data)
		for i := 0; i < dataLen; i += sql.Batchsize {
			end := i + sql.Batchsize
			if end > dataLen {
				end = dataLen
			}
			batch := data[i:end]
			tableBatchedDataMap[table] = append(tableBatchedDataMap[table], batch)
			workerCount++
		}
	}

	type Response struct {
		data map[statedb.CompositeKey]*version.Height
		err  error
	}

	type DBVersion struct {
		Key               []byte `json:"key"`
		BlockNumber       uint64 `json:"block_number"`
		TransactionNumber uint64 `json:"transaction_number"`
	}

	responseCh := make(chan Response, workerCount)
	var wg sync.WaitGroup

	for table, batch := range tableBatchedDataMap {
		for _, keys := range batch {
			wg.Add(1)
			go func(table string, keys [][]byte) {
				var data []DBVersion
				err := sql.Client.Table(table).Select("key", "block_number", "transaction_number").Where("key IN ?", keys).Find(&data).Error
				if err != nil {
					logger.Errorf("SQL GET VERSION err: %v\n", err)
					responseCh <- Response{err: err}
				}

				response := Response{data: make(map[statedb.CompositeKey]*version.Height)}
				for _, value := range data {
					compositeKey := statedb.CompositeKey{}
					compositeKey.Namespace, compositeKey.Key = decodeDataKey(value.Key)
					response.data[compositeKey] = &version.Height{
						BlockNum: value.BlockNumber,
						TxNum:    value.TransactionNumber,
					}
				}
				responseCh <- response
				wg.Done()
			}(table, keys)
		}
	}

	wg.Wait()
	close(responseCh)

	for range workerCount {
		response := <-responseCh
		if response.err != nil {
			return nil, response.err
		}
		maps.Copy(versionsMap, response.data)
	}

	return versionsMap, nil
}

func (sql *sqlSchema) ClearCache() {
	sql.cache.Clear()
}

type fullDBScanner struct {
	toSkip      func(namespace string) bool
	tables      []string
	tableCursor uint64
	cursor      uint64
	sql         *sqlSchema
}

func (sql *sqlSchema) NewFullDBScanner(skipNamespace func(namespace string) bool) (*fullDBScanner, error) {

	var tables []string
	err := sql.Client.Table("information_schema.tables").Where("table_schema = ?", sql.schema).Pluck("table_name", &tables).Error
	if err != nil {
		return nil, err
	}

	var filteredTables []string
	for _, table := range tables {
		tableNameWithSchema := fmt.Sprintf("%v.%v", sql.schema, table)

		if tableNameWithSchema != (Lifecycle{channel: sql.schema, peerId: sql.peerid}).TableName() {
			if !strings.HasSuffix(tableNameWithSchema, "_lifecycle") {
				continue
			}
			if sql.litePeerEnabled {
				continue
			}
		}

		if !skipNamespace(tableNameWithSchema) {
			filteredTables = append(filteredTables, tableNameWithSchema)
		}
	}

	logger.Info("Snapshotting tables : ", filteredTables)

	return &fullDBScanner{toSkip: skipNamespace, tables: filteredTables, tableCursor: 0, cursor: 0, sql: sql}, nil
}

func (s *fullDBScanner) Next() (*statedb.VersionedKV, error) {

	table := s.tables[s.tableCursor]
	cursor := s.cursor

	var data DBValue

	err := s.sql.Client.Table(table).Limit(1).Offset(int(cursor)).Find(&data).Error
	if err != nil {
		return nil, err
	}

	if data.Key == nil {
		s.tableCursor++
		s.cursor = 0
		if s.tableCursor == uint64(len(s.tables)) {
			return nil, nil
		}
		return s.Next()
	}

	s.cursor++

	if string(data.Key) == savePointKey {
		return s.Next()
	}

	namespace, key := decodeDataKey(data.Key)

	return &statedb.VersionedKV{
		CompositeKey: &statedb.CompositeKey{
			Namespace: namespace,
			Key:       key,
		},
		VersionedValue: &statedb.VersionedValue{
			Value:    data.DBValue,
			Metadata: data.DBMetadata,
			Version: &version.Height{
				BlockNum: data.BlockNumber,
				TxNum:    data.TransactionNumber,
			},
		},
	}, nil
}

func (s *fullDBScanner) Close() {
	fmt.Println("Closing FullDBScanner")
}
