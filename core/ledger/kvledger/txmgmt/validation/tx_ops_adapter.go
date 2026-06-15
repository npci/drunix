/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/
package validation

import (
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/statemetadata"
)

func prepareLtxOps(rwset *common.TxReadWriteSet, precedingUpdates *publicAndHashUpdates, db *privacyenabledstate.DB) (txOps, error) {
	txops := txOps{}
	if err := txops.applyLtxRwset(rwset); err != nil {
		return nil, err
	}
	for ck, keyop := range txops {
		// check if the final state of the key, value and metadata, is already present in the transaction, then skip
		// otherwise we need to retrieve latest state and merge in the current value or metadata update
		if keyop.isDelete() || keyop.isUpsertAndMetadataUpdate() {
			continue
		}

		// check if only value is updated in the current transaction then merge the metadata from last committed state
		if keyop.isOnlyUpsert() {
			latestMetadata, err := retrieveLatestMetadata(ck.ns, ck.coll, ck.key, precedingUpdates, db)
			if err != nil {
				return nil, err
			}
			keyop.metadata = latestMetadata
			continue
		}

		// only metadata is updated in the current transaction. Merge the value from the last committed state
		// If the key does not exist in the last state, make this key as noop in current transaction
		latestVal, err := retrieveLatestState(ck.ns, ck.coll, ck.key, precedingUpdates, db)
		if err != nil {
			return nil, err
		}
		if latestVal != nil {
			keyop.value = latestVal.Value
		} else {
			delete(txops, ck)
		}
	}
	// logger.Debugf("prepareTxOps() txops after final processing=%#v", spew.Sdump(txops))
	return txops, nil
}

func (txops txOps) applyLtxRwset(rwset *common.TxReadWriteSet) error {
	for _, nsRWSet := range rwset.NsRwset {
		ns := nsRWSet.Namespace
		for _, kvWrite := range nsRWSet.Rwset.Writes {
			txops.applyLtxKVWrite(ns, "", kvWrite)
		}
		for _, kvMetadataWrite := range nsRWSet.Rwset.MetadataWrites {
			if err := txops.applyLtxMetadata(ns, "", kvMetadataWrite); err != nil {
				return err
			}
		}

		// apply collection level kvwrite and kvMetadataWrite
		for _, collHashRWset := range nsRWSet.CollectionHashedRwset {
			coll := collHashRWset.CollectionName
			for _, hashedWrite := range collHashRWset.HashedRwset.HashedWrites {
				txops.applyLtxKVWrite(ns, coll,
					&common.KVWrite{
						Key:      string(hashedWrite.KeyHash),
						Value:    hashedWrite.ValueHash,
						IsDelete: rwsetutil.IsLtxKVWriteHashDelete(hashedWrite),
					},
				)
			}

			for _, metadataWrite := range collHashRWset.HashedRwset.MetadataWrites {
				if err := txops.applyLtxMetadata(ns, coll,
					&common.KVMetadataWrite{
						Key:     string(metadataWrite.KeyHash),
						Entries: metadataWrite.Entries,
					},
				); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (txops txOps) applyLtxKVWrite(ns, coll string, kvWrite *common.KVWrite) {
	if rwsetutil.IsLtxKVWriteDelete(kvWrite) {
		txops.delete(compositeKey{ns, coll, kvWrite.Key})
	} else {
		txops.upsert(compositeKey{ns, coll, kvWrite.Key}, kvWrite.Value)
	}
}

func (txops txOps) applyLtxMetadata(ns, coll string, metadataWrite *common.KVMetadataWrite) error {
	if metadataWrite.Entries == nil {
		txops.metadataDelete(compositeKey{ns, coll, metadataWrite.Key})
	} else {
		metadataBytes, err := statemetadata.LtxSerialize(metadataWrite.Entries)
		if err != nil {
			return err
		}
		txops.metadataUpdate(compositeKey{ns, coll, metadataWrite.Key}, metadataBytes)
	}
	return nil
}
