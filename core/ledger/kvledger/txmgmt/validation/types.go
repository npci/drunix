/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
Modifications Copyright National Payments Corporation of India
*/

package validation

import (
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	sparseblock "github.com/npci/drunix/common/sparseblocks"
	"github.com/npci/drunix/core/ledger/internal/version"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/rwsetutil"
)

// block is used to used to hold the information from its proto format to a structure
// that is more suitable/friendly for validation
type block struct {
	num uint64
	txs []*transaction

	// sparseblockchain:Filter for failed txns seq number
	sparseblockchainFilter sparseblock.SparseTxnFilterProto
	// sparseFilterExists
	sparseFilterExists bool
}

// transaction is used to hold the information from its proto format to a structure
// that is more suitable/friendly for validation
type transaction struct {
	indexInBlock            int
	id                      string
	rwset                   *rwsetutil.TxRwSet
	validationCode          peer.TxValidationCode
	containsPostOrderWrites bool
}

// publicAndHashUpdates encapsulates public and hash updates. The intended use of this to hold the updates
// that are to be applied to the statedb  as a result of the block commit
type publicAndHashUpdates struct {
	publicUpdates *privacyenabledstate.PubUpdateBatch
	hashUpdates   *privacyenabledstate.HashedUpdateBatch
}

// newPubAndHashUpdates constructs an empty PubAndHashUpdates
func newPubAndHashUpdates() *publicAndHashUpdates {
	return &publicAndHashUpdates{
		privacyenabledstate.NewPubUpdateBatch(),
		privacyenabledstate.NewHashedUpdateBatch(),
	}
}

// containsPvtWrites returns true if this transaction is not limited to affecting the public data only
func (t *transaction) containsPvtWrites() bool {
	for _, ns := range t.rwset.NsRwSets {
		for _, coll := range ns.CollHashedRwSets {
			if coll.PvtRwSetHash != nil {
				return true
			}
		}
	}
	return false
}

func (t *ltxTransaction) containsPvtWrites() bool {
	for _, ns := range t.rwset.NsRwset {
		for _, coll := range ns.CollectionHashedRwset {
			if coll.PvtRwsetHash != nil {
				return true
			}
		}
	}
	return false
}

// retrieveHash returns the hash of the private write-set present
// in the public data for a given namespace-collection
func (t *transaction) retrieveHash(ns string, coll string) []byte {
	if t.rwset == nil {
		return nil
	}
	for _, nsData := range t.rwset.NsRwSets {
		if nsData.NameSpace != ns {
			continue
		}

		for _, collData := range nsData.CollHashedRwSets {
			if collData.CollectionName == coll {
				return collData.PvtRwSetHash
			}
		}
	}
	return nil
}

func (t *ltxTransaction) retrieveHash(ns string, coll string) []byte {
	if t.rwset == nil {
		return nil
	}
	for _, nsData := range t.rwset.NsRwset {
		if nsData.Namespace != ns {
			continue
		}

		for _, collData := range nsData.CollectionHashedRwset {
			if collData.CollectionName == coll {
				return collData.PvtRwsetHash
			}
		}
	}
	return nil
}

// applyWriteSet adds (or deletes) the key/values present in the write set to the publicAndHashUpdates
func (u *publicAndHashUpdates) applyWriteSet(
	txRWSet *rwsetutil.TxRwSet,
	txHeight *version.Height,
	db *privacyenabledstate.DB,
	containsPostOrderWrites bool,
) error {
	u.publicUpdates.ContainsPostOrderWrites =
		u.publicUpdates.ContainsPostOrderWrites || containsPostOrderWrites
	txops, err := prepareTxOps(txRWSet, u, db)
	logger.Debugf("txops=%#v", txops)
	if err != nil {
		return err
	}
	for compositeKey, keyops := range txops {
		if compositeKey.coll == "" {
			ns, key := compositeKey.ns, compositeKey.key
			if keyops.isDelete() {
				u.publicUpdates.Delete(ns, key, txHeight)
			} else {
				u.publicUpdates.PutValAndMetadata(ns, key, keyops.value, keyops.metadata, txHeight)
			}
		} else {
			ns, coll, keyHash := compositeKey.ns, compositeKey.coll, []byte(compositeKey.key)
			if keyops.isDelete() {
				u.hashUpdates.Delete(ns, coll, keyHash, txHeight)
			} else {
				u.hashUpdates.PutValHashAndMetadata(ns, coll, keyHash, keyops.value, keyops.metadata, txHeight)
			}
		}
	}
	return nil
}

func (u *publicAndHashUpdates) applyLtxWriteSet(
	txRWSet *common.TxReadWriteSet,
	txHeight *version.Height,
	db *privacyenabledstate.DB,
	containsPostOrderWrites bool,
) error {
	u.publicUpdates.ContainsPostOrderWrites =
		u.publicUpdates.ContainsPostOrderWrites || containsPostOrderWrites
	txops, err := prepareLtxOps(txRWSet, u, db)
	logger.Debugf("txops=%#v", txops)
	if err != nil {
		return err
	}
	for compositeKey, keyops := range txops {
		if compositeKey.coll == "" {
			ns, key := compositeKey.ns, compositeKey.key
			if keyops.isDelete() {
				u.publicUpdates.Delete(ns, key, txHeight)
			} else {
				u.publicUpdates.PutValAndMetadata(ns, key, keyops.value, keyops.metadata, txHeight)
			}
		} else {
			ns, coll, keyHash := compositeKey.ns, compositeKey.coll, []byte(compositeKey.key)
			if keyops.isDelete() {
				u.hashUpdates.Delete(ns, coll, keyHash, txHeight)
			} else {
				u.hashUpdates.PutValHashAndMetadata(ns, coll, keyHash, keyops.value, keyops.metadata, txHeight)
			}
		}
	}
	return nil
}
