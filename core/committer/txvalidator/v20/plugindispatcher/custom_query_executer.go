/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/
package plugindispatcher

import (
	"github.com/npci/drunix/common/ledger"
	coreLedger "github.com/npci/drunix/core/ledger"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/statedb"
)

type CustomQueryExecuterCreator struct {
	queryExecuter statedb.VersionedDB
}

func NewCustomQueryExecuterCreator(queryExecuter statedb.VersionedDB) QueryExecutorCreator {

	return &CustomQueryExecuterCreator{queryExecuter: queryExecuter}
}

func (c *CustomQueryExecuterCreator) NewQueryExecutor() (coreLedger.QueryExecutor, error) {
	return QueryExecuter{queryExecuter: c.queryExecuter}, nil
}

type QueryExecuter struct {
	queryExecuter statedb.VersionedDB
}

// Done implements ledger.QueryExecutor.
func (q QueryExecuter) Done() {
}

// ExecuteQuery implements ledger.QueryExecutor.
func (q QueryExecuter) ExecuteQuery(namespace string, query string) (ledger.ResultsIterator, error) {
	panic("unimplemented")
}

// ExecuteQueryOnPrivateData implements ledger.QueryExecutor.
func (q QueryExecuter) ExecuteQueryOnPrivateData(namespace string, collection string, query string) (ledger.ResultsIterator, error) {
	panic("unimplemented")
}

// ExecuteQueryWithPagination implements ledger.QueryExecutor.
func (q QueryExecuter) ExecuteQueryWithPagination(namespace string, query string, bookmark string, pageSize int32) (coreLedger.QueryResultsIterator, error) {
	panic("unimplemented")
}

// GetPrivateData implements ledger.QueryExecutor.
func (q QueryExecuter) GetPrivateData(namespace string, collection string, key string) ([]byte, error) {
	panic("unimplemented")
}

// GetPrivateDataHash implements ledger.QueryExecutor.
func (q QueryExecuter) GetPrivateDataHash(namespace string, collection string, key string) ([]byte, error) {
	panic("unimplemented")
}

// GetPrivateDataMetadata implements ledger.QueryExecutor.
func (q QueryExecuter) GetPrivateDataMetadata(namespace string, collection string, key string) (map[string][]byte, error) {
	panic("unimplemented")
}

// GetPrivateDataMetadataByHash implements ledger.QueryExecutor.
func (q QueryExecuter) GetPrivateDataMetadataByHash(namespace string, collection string, keyhash []byte) (map[string][]byte, error) {
	panic("unimplemented")
}

// GetPrivateDataMultipleKeys implements ledger.QueryExecutor.
func (q QueryExecuter) GetPrivateDataMultipleKeys(namespace string, collection string, keys []string) ([][]byte, error) {
	panic("unimplemented")
}

// GetPrivateDataRangeScanIterator implements ledger.QueryExecutor.
func (q QueryExecuter) GetPrivateDataRangeScanIterator(namespace string, collection string, startKey string, endKey string) (ledger.ResultsIterator, error) {
	panic("unimplemented")
}

// GetState implements ledger.QueryExecutor.
func (q QueryExecuter) GetState(namespace string, key string) ([]byte, error) {
	versionedValue, err := q.queryExecuter.GetState(namespace, key)
	if err != nil {
		return nil, err
	}
	return versionedValue.Value, nil
}

// GetStateMetadata implements ledger.QueryExecutor.
func (q QueryExecuter) GetStateMetadata(namespace string, key string) (map[string][]byte, error) {
	panic("unimplemented")
}

// GetStateMultipleKeys implements ledger.QueryExecutor.
func (q QueryExecuter) GetStateMultipleKeys(namespace string, keys []string) ([][]byte, error) {

	versionedValues, err := q.queryExecuter.GetStateMultipleKeys(namespace, keys)
	if err != nil {
		return nil, err
	}

	versionedValueBytes := make([][]byte, len(versionedValues))
	for idx := range versionedValues {
		versionedValueBytes[idx] = versionedValues[idx].Value
	}

	return versionedValueBytes, nil
}

// GetStateRangeScanIterator implements ledger.QueryExecutor.
func (q QueryExecuter) GetStateRangeScanIterator(namespace string, startKey string, endKey string) (ledger.ResultsIterator, error) {
	panic("unimplemented")
}

// GetStateRangeScanIteratorWithPagination implements ledger.QueryExecutor.
func (q QueryExecuter) GetStateRangeScanIteratorWithPagination(namespace string, startKey string, endKey string, pageSize int32) (coreLedger.QueryResultsIterator, error) {
	panic("unimplemented")
}
