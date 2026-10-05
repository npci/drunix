/*
Copyright National Payments Corporation of India. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statesqldb

import (
	"errors"
	"testing"
	"time"
)

type fakeCloseSQLClient struct {
	closeCalls int
}

func (f *fakeCloseSQLClient) NewSchema(string, string, bool) (SqlSchema, error) {
	return nil, nil
}

func (f *fakeCloseSQLClient) Close() error {
	f.closeCalls++
	return nil
}

type fakeCloseSQLBatcher struct {
	closeCalls int
}

func (f *fakeCloseSQLBatcher) Get(string) (DBValue, error) {
	return DBValue{}, nil
}

func (f *fakeCloseSQLBatcher) Close() {
	f.closeCalls++
}

func TestSQLBatcherCloseIsIdempotent(t *testing.T) {
	originalBatchInterval := batchInterval
	batchInterval = time.Hour
	defer func() {
		batchInterval = originalBatchInterval
	}()

	batcher := NewSqlBatcher(&sqlSchema{})
	batcher.Close()
	batcher.Close()

	_, err := batcher.Get("key")
	if !errors.Is(err, errSqlBatcherClosed) {
		t.Fatalf("expected sql batcher closed error, got %v", err)
	}
}

func TestVersionedDBCloseStopsBatcher(t *testing.T) {
	batcher := &fakeCloseSQLBatcher{}
	db := &versionedDB{
		chainName: "test-channel",
		sqlSchema: &sqlSchema{SqlBatcher: batcher},
	}

	db.Close()

	if batcher.closeCalls != 1 {
		t.Fatalf("expected batcher to close once, got %d", batcher.closeCalls)
	}
}

func TestVersionedDBProviderCloseIsIdempotent(t *testing.T) {
	sqlClient := &fakeCloseSQLClient{}
	batcher1 := &fakeCloseSQLBatcher{}
	batcher2 := &fakeCloseSQLBatcher{}

	provider := &VersionedDBProvider{
		sqlClient: sqlClient,
		databases: map[string]*versionedDB{
			"channel-1": {
				chainName: "channel-1",
				sqlSchema: &sqlSchema{SqlBatcher: batcher1},
			},
			"channel-2": {
				chainName: "channel-2",
				sqlSchema: &sqlSchema{SqlBatcher: batcher2},
			},
		},
	}

	provider.Close()
	provider.Close()

	if sqlClient.closeCalls != 1 {
		t.Fatalf("expected SQL client to close once, got %d", sqlClient.closeCalls)
	}
	if batcher1.closeCalls != 1 {
		t.Fatalf("expected channel-1 batcher to close once, got %d", batcher1.closeCalls)
	}
	if batcher2.closeCalls != 1 {
		t.Fatalf("expected channel-2 batcher to close once, got %d", batcher2.closeCalls)
	}
}
