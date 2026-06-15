/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/
package kvledger

import (
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset/kvrwset"
	"github.com/npci/drunix/common/ledger/blkstorage"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/rwsetutil"
)

/*
DRUNIX:
	helper methods to process lite format transactions during reconciliation
*/ 

func retrieveRwsetForLtx(blkNum uint64, txNum uint64, blockStore *blkstorage.BlockStore) (*rwsetutil.TxRwSet, error) {
	// retrieve the txEnvelope from the block store so that the hash of
	// the pvtData can be retrieved for comparison
	txEnvelope, err := blockStore.RetrieveTxByBlockNumTranNum(blkNum, txNum)
	if err != nil {
		return nil, err
	}

	results := txEnvelope.LeanEnv.Results
	rwset := GetNsRwSetFromLtx(results.NsRwset)

	txRWSet := &rwsetutil.TxRwSet{
		NsRwSets: rwset,
	}

	return txRWSet, nil

}

func GetNsRwSetFromLtx(ns []*common.NsReadWriteSet) []*rwsetutil.NsRwSet {
	txRwList := []*rwsetutil.NsRwSet{}

	for _, namespace := range ns {

		nsrwset := &rwsetutil.NsRwSet{
			NameSpace:        namespace.Namespace,
			KvRwSet:          GetKvRwSetFromLtx(namespace.GetRwset()),
			CollHashedRwSets: []*rwsetutil.CollHashedRwSet{},
		}

		for _, collhash := range namespace.CollectionHashedRwset {
			nsrwset.CollHashedRwSets = append(nsrwset.CollHashedRwSets, GetCollHashRwSetFromLtx(collhash))
		}

		txRwList = append(txRwList, nsrwset)
	}

	return txRwList
}

func GetKvRwSetFromLtx(rwset *common.KVRWSet) *kvrwset.KVRWSet {
	kvrws := &kvrwset.KVRWSet{
		Reads:            []*kvrwset.KVRead{},
		RangeQueriesInfo: []*kvrwset.RangeQueryInfo{},
		Writes:           []*kvrwset.KVWrite{},
		MetadataWrites:   []*kvrwset.KVMetadataWrite{},
	}

	for _, rd := range rwset.Reads {
		cVersion := GetVersionFromLtx(rd.Version)
		kvrws.Reads = append(kvrws.Reads, &kvrwset.KVRead{
			Key:     rd.Key,
			Version: cVersion,
		})
	}
	for _, wr := range rwset.Writes {
		kvrws.Writes = append(kvrws.Writes, &kvrwset.KVWrite{
			Key:      wr.Key,
			IsDelete: wr.IsDelete,
			Value:    wr.Value,
		})
	}

	return kvrws
}

func GetVersionFromLtx(kVersion *common.Version) *kvrwset.Version {

	if kVersion == nil {
		return nil
	} else {
		return &kvrwset.Version{
			BlockNum: kVersion.BlockNum,
			TxNum:    kVersion.TxNum,
		}
	}
}

func GetCollHashRwSetFromLtx(hrwset *common.CollectionHashedReadWriteSet) *rwsetutil.CollHashedRwSet {

	cNs := &rwsetutil.CollHashedRwSet{
		CollectionName: hrwset.CollectionName,
		HashedRwSet:    GetHashRWSetFromLtx(hrwset.HashedRwset),
		PvtRwSetHash:   hrwset.PvtRwsetHash,
	}

	return cNs
}

func GetHashRWSetFromLtx(hrwset *common.HashedRWSet) *kvrwset.HashedRWSet {

	cNs := &kvrwset.HashedRWSet{
		HashedReads:    []*kvrwset.KVReadHash{},
		HashedWrites:   []*kvrwset.KVWriteHash{},
		MetadataWrites: []*kvrwset.KVMetadataWriteHash{},
	}

	for _, rd := range hrwset.HashedReads {
		cVersion := GetVersionFromLtx(rd.Version)
		cNs.HashedReads = append(cNs.HashedReads, &kvrwset.KVReadHash{
			KeyHash: rd.KeyHash,
			Version: cVersion,
		})
	}
	for _, wr := range hrwset.HashedWrites {
		cNs.HashedWrites = append(cNs.HashedWrites, &kvrwset.KVWriteHash{
			KeyHash:   wr.KeyHash,
			IsDelete:  wr.IsDelete,
			ValueHash: wr.ValueHash,
		})
	}

	return cNs
}
