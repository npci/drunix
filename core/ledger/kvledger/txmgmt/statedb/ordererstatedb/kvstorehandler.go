/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/
/*
	DRUNIX
*/

package ordererstatedb

import (
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/npci/drunix/common/flogging"
	"github.com/npci/drunix/common/ledger/util/leveldbhelper"
	"github.com/npci/drunix/core/ledger/internal/version"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/statedb"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/statedb/stateleveldb"
)

var logger = flogging.MustGetLogger("ordererstatedb")

var (
	dataKeyPrefix          = []byte{'d'}
	dataKeyStopper         = []byte{'e'}
	nsKeySep               = []byte{0x00}
	lastKeyIndicator       = byte(0x01)
	savePointKey           = []byte{'s'}
	maxDataImportBatchSize = 4 * 1024 * 1024
)

const (
	nsJoiner       = "$$"
	pvtDataPrefix  = "p"
	hashDataPrefix = "h"
)

type TxnDetails struct {
	Indx uint64
	// Env           *cb.Envelope
	TxnEnvBytes   []byte
	TxnMVCCStatus peer.TxValidationCode `json:"txnMVCCStatus"`
	OrgList       []string
}

type OrdererDBHandler struct {
	channelId string
	kvHandler statedb.KVersionStoreHandler
	db        *leveldbhelper.DB
}

func encodeDataKey(ns, key string) []byte {
	k := append(dataKeyPrefix, []byte(ns)...)
	k = append(k, nsKeySep...)
	return append(k, []byte(key)...)
}

func deriveHashedDataNs(namespace, collection string) string {
	return namespace + nsJoiner + hashDataPrefix + collection
}

type KVDBProvider struct {
	versionedDBProvider *stateleveldb.VersionedDBProvider
	status              string //TODO: Implement Singleton Pattern
}

// NewKVDBProvider creates and returns an KVDBProvider based on the provided dbPath
func NewKVDBProvider(dbPath string) (*KVDBProvider, error) {
	vDbProvider, err := stateleveldb.NewVersionedDBProvider(dbPath)
	if err != nil {
		return nil, err
	}
	return &KVDBProvider{
		versionedDBProvider: vDbProvider,
		status:              "",
	}, nil
}

// NewOrdererDBHandler creates and returns an OrdererDBHandler based on the provided channelId
func (kvDBProvider *KVDBProvider) NewOrdererDBHandler(channelId string) (OrdererDBHandler, error) {
	kvStoreHandler, err := kvDBProvider.versionedDBProvider.GetKVHandle(channelId)
	if err != nil {
		logger.Panicf("error received : %v", err)
		return OrdererDBHandler{}, err
	}

	return OrdererDBHandler{
		channelId: channelId,
		kvHandler: kvStoreHandler,
	}, nil
}

// NewKVStoreHandler returns a KVStoreHandler
func NewKVStoreHandler(channelID string) (statedb.KVersionStoreHandler, error) {
	// TODO: Handle newDB & import from snapshot
	vDbProvider, err := stateleveldb.NewVersionedDBProvider(channelID)
	if err != nil {
		return nil, err
	}
	versionDB, err := vDbProvider.GetKVHandle(channelID)
	if err != nil {
		return nil, err
	}

	return versionDB, nil
}

// ValidateReadSet checks validate each key-value read in the transaction's read set
func (od *OrdererDBHandler) ValidateReadSet(fatTxnDetailsMVCCMap map[string]*statedb.VersionMVCC, nsRwSet *cb.NsReadWriteSet, transaction *TxnDetails) (bool, error) {
	logger.Debugf("Starting validating nsRwSet.KvRwSet.Reads: %v", len(nsRwSet.Rwset.Reads))
	for i, kvRead := range nsRwSet.Rwset.Reads {
		logger.Debugf("Validating key: %v at index: %v with version: %v\n", kvRead.Key, i, kvRead.Version)
		if valid, err := od.validateKVRead(fatTxnDetailsMVCCMap, nsRwSet.Namespace, kvRead, transaction); !valid || err != nil {
			return valid, err
		}
	}

	return true, nil
}

func (od *OrdererDBHandler) validateKVRead(fatTxnDetailsMVCCMap map[string]*statedb.VersionMVCC, ns string, kvRead *cb.KVRead, transaction *TxnDetails) (bool, error) {
	readVersion := rwsetutil.NewLiteVersion(kvRead.Version) // 0 // key version within the txn from FatBlock

	var (
		committedVersion *version.Height
		err              error
	)

	txnDetailsMVCCMap := fatTxnDetailsMVCCMap

	key := string(encodeDataKey(ns, kvRead.Key))

	_, ok := txnDetailsMVCCMap[key]
	if ok {
		// committedVersion = v.Version
		transaction.TxnMVCCStatus = peer.TxValidationCode_MVCC_READ_CONFLICT
		logger.Debugf("Invalidated due to nil version came twice for read-sets")
		return false, nil
	} else {
		committedVersion, err = od.kvHandler.GetKeyVersion(ns, kvRead.Key)
		logger.Debugf("kvRead.Key: %v\n", kvRead.Key)
		if err != nil {
			logger.Debugf("could not find key: %v, in db", kvRead.Key)
			return true, nil
		}
	}

	logger.Debugf("Comparing readset version to committed version, namespace :%v,  key : %v, readVersion : %v, committedVersion : %v", ns, kvRead.Key, readVersion, committedVersion)
	/* If key came for the first time and in same block
	   same txn with same but with nil version */
	if readVersion == nil && committedVersion != nil {
		transaction.TxnMVCCStatus = peer.TxValidationCode_MVCC_READ_CONFLICT
		logger.Debugf("Invalidated due to nil version came twice for read-sets")
		return false, nil
	}

	if !version.AreSame(committedVersion, readVersion) {
		transaction.TxnMVCCStatus = peer.TxValidationCode_MVCC_READ_CONFLICT
		logger.Debugf("Transaction invalidation due to version mismatch, readset version does not match committed version,namespace : %v, key :%v, readVersion: %v, committedVersion: %v", ns, kvRead.Key, readVersion, committedVersion)
		return false, nil
	}

	return true, nil
}

func (od *OrdererDBHandler) ValidateNsHashedReadSets(fatTxnDetailsMVCCMap map[string]*statedb.VersionMVCC, nsRwSet *cb.NsReadWriteSet, transaction *TxnDetails) (bool, error) {
	logger.Debugf("number of collections: %v", len(nsRwSet.CollectionHashedRwset))
	for _, collHashedRWSet := range nsRwSet.CollectionHashedRwset {
		if valid, err := od.validateCollHashedReadSet(fatTxnDetailsMVCCMap, nsRwSet.Namespace, collHashedRWSet.CollectionName, collHashedRWSet.HashedRwset.HashedReads, transaction); !valid || err != nil {
			return valid, err
		}
	}

	return true, nil
}

func (od *OrdererDBHandler) validateCollHashedReadSet(fatTxnDetailsMVCCMap map[string]*statedb.VersionMVCC, ns, coll string, kvReadHashes []*cb.KVReadHash, transaction *TxnDetails) (bool, error) {
	logger.Debugf("Namespace of the collection Hashed set: %v, for collection: %v with length: %v", ns, coll, len(kvReadHashes))
	for _, kvReadHash := range kvReadHashes {
		if valid, err := od.validateKVReadHash(fatTxnDetailsMVCCMap, ns, coll, kvReadHash, transaction); !valid || err != nil {
			return valid, err
		}
	}

	return true, nil
}

func (od *OrdererDBHandler) validateKVReadHash(fatTxnDetailsMVCCMap map[string]*statedb.VersionMVCC, ns, coll string, kvReadHash *cb.KVReadHash, transaction *TxnDetails) (bool, error) {
	readHashVersion := rwsetutil.NewLiteVersion(kvReadHash.Version)

	var (
		committedVersion *version.Height
		err              error
	)

	txnDetailsMVCCMap := fatTxnDetailsMVCCMap
	key := string(encodeDataKey(deriveHashedDataNs(ns, coll), string(kvReadHash.KeyHash)))
	_, ok := txnDetailsMVCCMap[key]
	if ok {
		// committedVersion = v.Version
		transaction.TxnMVCCStatus = peer.TxValidationCode_MVCC_READ_CONFLICT
		logger.Debugf("Invalidated due to nil version came twice for read-sets")
		return false, nil
	} else {
		committedVersion, err = od.kvHandler.GetKeyVersion(ns, string(kvReadHash.KeyHash))
		if err != nil {
			logger.Debugf("could not find key: %v in db", key)
			return true, nil
		}
	}

	logger.Debugf("Comparing readset version to committed version, namespace: %v, collection :%v,  keyHash : %v, readVersion : %v, committedVersion : %v", ns, coll, kvReadHash.KeyHash, readHashVersion, committedVersion)
	/* If key came for the first time and in same block
	   same txn with same but with nil version */
	if readHashVersion == nil && committedVersion != nil {
		transaction.TxnMVCCStatus = peer.TxValidationCode_MVCC_READ_CONFLICT
		logger.Debugf("Invalidated due to nil version came twice for hashed-sets")
		return false, nil
	}
	if !version.AreSame(committedVersion, readHashVersion) {
		transaction.TxnMVCCStatus = peer.TxValidationCode_MVCC_READ_CONFLICT

		logger.Debugf("Transaction invalidation due to hash version mismatch, readset version does not match committed version ,namespace : %v, collection : %v keyHash :%v, readVersion: %v, committedVersion: %v", ns, coll, kvReadHash.KeyHash, readHashVersion, committedVersion)
		return false, nil
	}

	return true, nil
}

func (od *OrdererDBHandler) ApplyWriteSet(fatTxnDetailsMVCCMap map[string]*statedb.VersionMVCC, ns string, kvWrites []*cb.KVWrite, version *version.Height) (bool, error) {
	logger.Debugf("Trying to apply for kvWrites with length: %v", len(kvWrites))
	txnDetailsMVCC := fatTxnDetailsMVCCMap
	// updates := make(map[string]*statedb.VersionedValue, 0)
	for _, kvWrite := range kvWrites {
		logger.Debugf("Applying key: %v with value:%v with version: %v", kvWrite.Key, kvWrite.Value, *version)
		newKey := encodeDataKey(ns, kvWrite.Key)
		// err := kVersionHandler.Put(newKey, *version)
		txnDetailsMVCC[string(newKey)] = &statedb.VersionMVCC{
			Version:   version,
			Is_Delete: kvWrite.GetIsDelete(),
		}
	}

	// To be batched ****************
	// err := od.kvHandler.ApplyOrdererUpdates(ns, updates, version)
	// if err != nil {
	// 	return false, err
	// }

	return true, nil
}

func (od *OrdererDBHandler) ApplyWriteHashSet(fatTxnDetailsMVCCMap map[string]*statedb.VersionMVCC, ns string, collHashedRWSets []*cb.CollectionHashedReadWriteSet, version *version.Height) (bool, error) {
	logger.Debugf("Trying to apply for collHashedRWSets with length: %v", len(collHashedRWSets))
	txnDetailsMVCC := fatTxnDetailsMVCCMap
	for _, collHashedRWSet := range collHashedRWSets {
		logger.Debugf("Trying to apply for collHashedRWSet.HashedRwSet.HashedWrites with length: %v", len(collHashedRWSet.HashedRwset.HashedWrites))
		for _, kvWriteHash := range collHashedRWSet.HashedRwset.HashedWrites {
			logger.Debugf("Applying keyHash: %v with valueHash :%v with version: %v", kvWriteHash.KeyHash, kvWriteHash.ValueHash, *version)
			newKey := encodeDataKey(deriveHashedDataNs(ns, collHashedRWSet.CollectionName), string(kvWriteHash.KeyHash))
			txnDetailsMVCC[string(newKey)] = &statedb.VersionMVCC{
				Version:   version,
				Is_Delete: kvWriteHash.GetIsDelete(),
			}
		}
	}

	// To be batched **************
	// err := od.kvHandler.ApplyOrdererUpdates(ns, updates, version)
	// if err != nil {
	// 	logger.Debugf("failed to apply: %v", err)
	// 	return false, err
	// }

	return true, nil
}

// Updates are batched on the basis of one fat block
func (od *OrdererDBHandler) ApplyBatch(fatTxnDetailsMVCCMap map[string]*statedb.VersionMVCC) (bool, error) {

	// To be batched **************
	err := od.kvHandler.ApplyOrdererUpdates(fatTxnDetailsMVCCMap)
	if err != nil {
		logger.Debugf("failed to apply: %v", err)
		return false, err
	}

	return true, nil
}
