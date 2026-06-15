/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/

package validation

import (
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/npci/drunix/core/ledger/internal/version"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/statedb"
	"github.com/npci/drunix/protoutil"
)

func (v *validator) preLoadCommittedVersionOfLtxRSet(blk *ltxBlock) error {
	// Collect both public and hashed keys in read sets of all transactions in a given block
	var pubKeys []*statedb.CompositeKey
	var hashedKeys []*privacyenabledstate.HashedCompositeKey

	// pubKeysMap and hashedKeysMap are used to avoid duplicate entries in the
	// pubKeys and hashedKeys. Though map alone can be used to collect keys in
	// read sets and pass as an argument in LoadCommittedVersionOfPubAndHashedKeys(),
	// array is used for better code readability. On the negative side, this approach
	// might use some extra memory.
	pubKeysMap := make(map[statedb.CompositeKey]interface{})
	hashedKeysMap := make(map[privacyenabledstate.HashedCompositeKey]interface{})

	for _, tx := range blk.txs {
		for _, nsRWSet := range tx.rwset.NsRwset {
			for _, kvRead := range nsRWSet.Rwset.Reads {
				compositeKey := statedb.CompositeKey{
					Namespace: nsRWSet.Namespace,
					Key:       kvRead.Key,
				}
				if _, ok := pubKeysMap[compositeKey]; !ok {
					pubKeysMap[compositeKey] = nil
					pubKeys = append(pubKeys, &compositeKey)
				}

			}
			for _, colHashedRwSet := range nsRWSet.CollectionHashedRwset {
				for _, kvHashedRead := range colHashedRwSet.HashedRwset.HashedReads {
					hashedCompositeKey := privacyenabledstate.HashedCompositeKey{
						Namespace:      nsRWSet.Namespace,
						CollectionName: colHashedRwSet.CollectionName,
						KeyHash:        string(kvHashedRead.KeyHash),
					}
					if _, ok := hashedKeysMap[hashedCompositeKey]; !ok {
						hashedKeysMap[hashedCompositeKey] = nil
						hashedKeys = append(hashedKeys, &hashedCompositeKey)
					}
				}
			}
		}
	}

	// Load committed version of all keys into a cache
	if len(pubKeys) > 0 || len(hashedKeys) > 0 {
		err := v.db.LoadCommittedVersionsOfPubAndHashedKeys(pubKeys, hashedKeys)
		if err != nil {
			return err
		}
	}

	return nil
}

func (v *validator) validateAndPrepareLtxBatch(blk *ltxBlock, doMVCCValidation bool) (*publicAndHashUpdates, []*AppInitiatedPurgeUpdate, error) {
	// Check whether statedb implements BulkOptimizable interface. For now,
	// only CouchDB implements BulkOptimizable to reduce the number of REST
	// API calls from peer to CouchDB instance.
	if v.db.IsBulkOptimizable() {
		err := v.preLoadCommittedVersionOfLtxRSet(blk)
		if err != nil {
			return nil, nil, err
		}
	}

	updates := newPubAndHashUpdates()
	purgeTracker := newPvtdataPurgeTracker()

	blockNumber := blk.num
	if blk.sparseFilterExists && blk.num != 0 {
		blockNumber = blk.sparseblockchainFilter.FatBlockNumber
	}

	for _, tx := range blk.txs {

		var err error
		// DRUNIX : perform MVCC only if VSCC is valid
		if tx.validationCode == peer.TxValidationCode_VALID {
			if tx.validationCode, err = v.validateEndorserLtX(tx.rwset, doMVCCValidation, updates); err != nil {
				return nil, nil, err
			}
		}

		if tx.validationCode == peer.TxValidationCode_VALID {
			indexInFatBlock := uint64(tx.indexInBlock)
			if blk.sparseFilterExists && blk.num != 0 {
				indexInFatBlock = blk.sparseblockchainFilter.TxnInfoList[tx.indexInBlock].IndexInFatBlock
			}
			committingTxHeight := version.NewHeight(blockNumber, indexInFatBlock)
			if err := updates.applyLtxWriteSet(tx.rwset, committingTxHeight, v.db, tx.containsPostOrderWrites); err != nil {
				logger.Error("applying write set failed for tx %d: %s", tx.indexInBlock, err)
				return nil, nil, err
			}
			logger.Debugf("Block [%d] Transaction index [%d] TxId [%s] marked as valid by state validator. ContainsPostOrderWrites [%t]", blk.num, tx.indexInBlock, tx.id, tx.containsPostOrderWrites)

			// DRUNIX-LTF-TODO
			// purgeTracker.update(tx.rwset, committingTxHeight)
		} else {
			logger.Warningf("Block [%d] Transaction index [%d] TxId [%s] marked as invalid by state validator. Reason code [%s]",
				blk.num, tx.indexInBlock, tx.id, tx.validationCode.String())
		}
	}
	return updates, purgeTracker.getUpdates(), nil
}

func (v *validator) validateEndorserLtX(
	txRWSet *common.TxReadWriteSet,
	doMVCCValidation bool,
	updates *publicAndHashUpdates) (peer.TxValidationCode, error) {
	validationCode := peer.TxValidationCode_VALID
	var err error
	// mvcc validation, may invalidate transaction
	if doMVCCValidation {
		validationCode, err = v.validateLtx(txRWSet, updates)
	}
	return validationCode, err
}

func (v *validator) validateLtx(txRWSet *common.TxReadWriteSet, updates *publicAndHashUpdates) (peer.TxValidationCode, error) {
	// Uncomment the following only for local debugging. Don't want to print data in the logs in production
	// logger.Debugf("validateTx - validating txRWSet: %s", spew.Sdump(txRWSet))
	for _, nsRWSet := range txRWSet.NsRwset {
		ns := nsRWSet.Namespace
		// Validate public reads
		if valid, err := v.validateLtxReadSet(ns, nsRWSet.Rwset.Reads, updates.publicUpdates); !valid || err != nil {
			if err != nil {
				return peer.TxValidationCode(-1), err
			}
			return peer.TxValidationCode_MVCC_READ_CONFLICT, nil
		}
		// Validate range queries for phantom items
		if valid, err := v.validateLtxRangeQueries(ns, nsRWSet.Rwset.RangeQueriesInfo, updates.publicUpdates); !valid || err != nil {
			if err != nil {
				return peer.TxValidationCode(-1), err
			}
			return peer.TxValidationCode_PHANTOM_READ_CONFLICT, nil
		}
		// Validate hashes for private reads
		if valid, err := v.validateNsLtxHashedReadSets(ns, nsRWSet.CollectionHashedRwset, updates.hashUpdates); !valid || err != nil {
			if err != nil {
				return peer.TxValidationCode(-1), err
			}
			return peer.TxValidationCode_MVCC_READ_CONFLICT, nil
		}
	}
	return peer.TxValidationCode_VALID, nil
}

func (v *validator) validateLtxReadSet(ns string, kvReads []*common.KVRead, updates *privacyenabledstate.PubUpdateBatch) (bool, error) {
	for _, kvRead := range kvReads {
		if valid, err := v.validateLtxKVRead(ns, kvRead, updates); !valid || err != nil {
			return valid, err
		}
	}
	return true, nil
}

func (v *validator) validateLtxKVRead(ns string, kvRead *common.KVRead, updates *privacyenabledstate.PubUpdateBatch) (bool, error) {
	readVersion := rwsetutil.NewLtxVersion(kvRead.Version)
	if updates.Exists(ns, kvRead.Key) {
		logger.Warnw("Transaction invalidation due to version mismatch, key in readset has been updated in a prior transaction in this block",
			"namespace", ns, "key", kvRead.Key, "readVersion", readVersion)
		return false, nil
	}
	committedVersion, err := v.db.GetVersion(ns, kvRead.Key)
	if err != nil {
		return false, err
	}

	logger.Debugw("Comparing readset version to committed version",
		"namespace", ns, "key", kvRead.Key, "readVersion", readVersion, "committedVersion", committedVersion)

	if !version.AreSame(committedVersion, readVersion) {
		logger.Warnw("Transaction invalidation due to version mismatch, readset version does not match committed version",
			"namespace", ns, "key", kvRead.Key, "readVersion", readVersion, "committedVersion", committedVersion)
		return false, nil
	}
	return true, nil
}

func (v *validator) validateLtxRangeQueries(ns string, rangeQueriesInfo []*common.RangeQueryInfo, updates *privacyenabledstate.PubUpdateBatch) (bool, error) {
	for _, rqi := range rangeQueriesInfo {
		if valid, err := v.validateLtxRangeQuery(ns, rqi, updates); !valid || err != nil {
			return valid, err
		}
	}
	return true, nil
}

func (v *validator) validateLtxRangeQuery(ns string, rangeQueryInfo *common.RangeQueryInfo, updates *privacyenabledstate.PubUpdateBatch) (bool, error) {
	logger.Debugf("validateRangeQuery: ns=%s, rangeQueryInfo=%s", ns, rangeQueryInfo)

	// If during simulation, the caller had not exhausted the iterator so
	// rangeQueryInfo.EndKey is not actual endKey given by the caller in the range query
	// but rather it is the last key seen by the caller and hence the combinedItr should include the endKey in the results.
	includeEndKey := !rangeQueryInfo.ItrExhausted

	combinedItr, err := newCombinedIterator(v.db, updates.UpdateBatch,
		ns, rangeQueryInfo.StartKey, rangeQueryInfo.EndKey, includeEndKey)
	if err != nil {
		return false, err
	}
	defer combinedItr.Close()
	var qv rangeQueryValidator
	if rangeQueryInfo.GetReadsMerkleHashes() != nil {
		logger.Debug(`Hashing results are present in the range query info hence, initiating hashing based validation`)
		qv = &rangeQueryHashValidator{hashFunc: v.hashFunc}
	} else {
		logger.Debug(`Hashing results are not present in the range query info hence, initiating raw KVReads based validation`)
		qv = &rangeQueryResultsValidator{}
	}

	/*
		DRUNIX:
			rangeQueryValidator interface accepts `*kvrwset.RangeQueryInfo`, to make it accept lite RangeQueryInfo we have to modify the interface which is not advisible at this point. This is a TODO in future, check if any other pkg is dependent upon this interface and then take a decision.
			Changing common rangeQueryInfo to kvrwset rangeQueryInfo and then making the rangeQueryValidator interface call

			TODO : This needs to be tested.
	*/
	rqiBytes, err := protoutil.Marshal(rangeQueryInfo)
	if err != nil {
		logger.Debugf(`failed marshalling of lite range query info : `, err)
		qv = &rangeQueryResultsValidator{}
	}

	kvrqi := &kvrwset.RangeQueryInfo{}

	err = proto.Unmarshal(rqiBytes, kvrqi)
	if err != nil {
		logger.Debugf(`failed unmarshalling of lite range query info to kvrwset range query info : `, err)
		qv = &rangeQueryResultsValidator{}
	}

	if err := qv.init(kvrqi, combinedItr); err != nil {
		return false, err
	}
	return qv.validate()
}

func (v *validator) validateNsLtxHashedReadSets(ns string, collHashedRWSets []*common.CollectionHashedReadWriteSet,
	updates *privacyenabledstate.HashedUpdateBatch) (bool, error) {
	for _, collHashedRWSet := range collHashedRWSets {
		if valid, err := v.validateLtxCollHashedReadSet(ns, collHashedRWSet.CollectionName, collHashedRWSet.HashedRwset.HashedReads, updates); !valid || err != nil {
			return valid, err
		}
	}
	return true, nil
}

func (v *validator) validateLtxCollHashedReadSet(ns, coll string, kvReadHashes []*common.KVReadHash,
	updates *privacyenabledstate.HashedUpdateBatch) (bool, error) {
	for _, kvReadHash := range kvReadHashes {
		if valid, err := v.validateLtxKVReadHash(ns, coll, kvReadHash, updates); !valid || err != nil {
			return valid, err
		}
	}
	return true, nil
}

func (v *validator) validateLtxKVReadHash(ns, coll string, kvReadHash *common.KVReadHash, updates *privacyenabledstate.HashedUpdateBatch) (bool, error) {
	readHashVersion := rwsetutil.NewLtxVersion(kvReadHash.Version)
	if updates.Contains(ns, coll, kvReadHash.KeyHash) {
		logger.Warnw("Transaction invalidation due to hash version mismatch, hash key in readset has been updated in a prior transaction in this block",
			"namespace", ns, "collection", coll, "keyHash", kvReadHash.KeyHash, "readHashVersion", readHashVersion)
		return false, nil
	}
	committedVersion, err := v.db.GetKeyHashVersion(ns, coll, kvReadHash.KeyHash)
	if err != nil {
		return false, err
	}

	logger.Debugw("Comparing hash readset version to committed version",
		"namespace", ns, "collection", coll, "keyHash", kvReadHash.KeyHash, "readVersion", readHashVersion, "committedVersion", committedVersion)

	if !version.AreSame(committedVersion, readHashVersion) {
		logger.Warnw("Transaction invalidation due to hash version mismatch, readset version does not match committed version",
			"namespace", ns, "collection", coll, "keyHash", kvReadHash.KeyHash, "readVersion", readHashVersion, "committedVersion", committedVersion)
		return false, nil
	}
	return true, nil
}

// DRUNIX-LTF-TODO : new method in v2.5.13 have to make it compatible with ltx
// func (p *pvtdataPurgeTracker) ltxUpdate(rwset *common.TxReadWriteSet, version *version.Height) {
// 	for _, nsRwSets := range rwset.NsRwset {
// 		for _, collHashedRwSet := range nsRwSets.CollectionHashedRwset {
// 			for _, hashedWrite := range collHashedRwSet.HashedRwset.HashedWrites {

// 				ck := privacyenabledstate.HashedCompositeKey{
// 					Namespace:      nsRwSets.Namespace,
// 					CollectionName: collHashedRwSet.CollectionName,
// 					KeyHash:        string(hashedWrite.GetKeyHash()),
// 				}

// 				if hashedWrite.IsPurge {
// 					p.m[ck] = &AppInitiatedPurgeUpdate{
// 						CompositeKey: &ck,
// 						Version:      version,
// 					}
// 				}
// 			}
// 		}
// 	}
// }
