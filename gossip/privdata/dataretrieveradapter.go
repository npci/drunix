/*
Copyright National Payments Corporation of India. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package privdata

import (
	protosgossip "github.com/hyperledger/fabric-protos-go/gossip"
	"github.com/npci/drunix/core/ledger"
	"github.com/npci/drunix/gossip/privdata/common"
	"github.com/npci/drunix/gossip/util"
	"github.com/pkg/errors"
)

// DRUNIX : to fetch the pvt data from KVStore
// CollectionRWSet retrieves for give digest relevant private data if
// available otherwise returns nil, bool which is true if data fetched from ledger and false if was fetched from transient store, and an error
func (dr *dataRetriever) CollectionRWSetFromKVStore(digests []*protosgossip.PvtDataDigest, blockNum uint64) (Dig2PvtRWSetWithConfig, bool, error) {
	height, err := dr.committer.LedgerHeight()
	if err != nil {
		// if there is an error getting info from the ledger, we need to try to read from transient store
		return nil, false, errors.Wrap(err, "wasn't able to read ledger height")
	}

	// The condition may be true for either commit or reconciliation case when another peer sends a request to retrieve private data.
	// For the commit case, get the private data from the transient store because the block has not been committed.
	// For the reconciliation case, this peer is further behind the ledger height than the peer that requested for the private data.
	// In this case, the ledger does not have the requested private data. Also, the data cannot be queried in the transient store,
	// as the txID in the digest will be missing.
	if height <= blockNum { // Check whenever current ledger height is equal or below block sequence num.
		dr.logger.Debug("Current ledger height ", height, "is below requested block sequence number",
			blockNum, "retrieving private data from transient store")

		results := make(Dig2PvtRWSetWithConfig)
		iterMap := make(map[string]*protosgossip.PvtDataDigest)
		rwsetKeysMap := make(map[string][]ledger.PvtNsCollFilter)
		for _, dig := range digests {
			// skip retrieving from transient store if txid is not available
			if dig.TxId == "" {
				dr.logger.Infof("Skip querying transient store for chaincode %s, collection name %s, block number %d, sequence in block %d, "+
					"as the txid is missing, perhaps because it is a reconciliation request",
					dig.Namespace, dig.Collection, blockNum, dig.SeqInBlock)

				continue
			}
			iterMap[dig.TxId] = dig
			filter := map[string]ledger.PvtCollFilter{
				dig.Namespace: map[string]bool{
					dig.Collection: true,
				},
			}
			if len(rwsetKeysMap[dig.TxId]) == 0 {
				rwsetKeysMap[dig.TxId] = []ledger.PvtNsCollFilter{filter}
			} else {
				rwsetKeysMap[dig.TxId] = append(rwsetKeysMap[dig.TxId], filter)
			}
		}

		transientstoreData, err := dr.store.KVStore.GetTxPvtRWSetByTxidV2(rwsetKeysMap)
		if err != nil {
			return results, false, err
		}

		for _, dig := range iterMap {
			if len(transientstoreData[dig.TxId]) == 0 {
				// err := errors.Errorf("error getting next element out of private data iterator, namespace <%s>"+
				// 	", collection name <%s>, txID <%s>, due to <%s>", dig.Namespace, dig.Collection, dig.TxId, err)
				// dr.logger.Errorf("couldn't read from transient store private read-write set, "+"digest %+v, because of %s", dig, err)
				continue
			}
			pvtRWSetWithConfig := &util.PrivateRWSetWithConfig{}
			maxEndorsedAt := uint64(0)

			for _, resArr := range transientstoreData[dig.TxId] {
				for _, res := range resArr {
					rws := res.PvtSimulationResultsWithConfig
					if rws == nil {
						dr.logger.Debug("Skipping nil PvtSimulationResultsWithConfig received at block height", res.ReceivedAtBlockHeight)
						continue
					}
					txPvtRWSet := rws.PvtRwset
					if txPvtRWSet == nil {
						dr.logger.Debug("Skipping empty PvtRwset of PvtSimulationResultsWithConfig received at block height", res.ReceivedAtBlockHeight)
						continue
					}

					colConfigs, found := rws.CollectionConfigs[dig.Namespace]
					if !found {
						dr.logger.Error("No collection config was found for chaincode", dig.Namespace, "collection name",
							dig.Collection, "txID", dig.TxId)
						continue
					}

					configs := extractCollectionConfig(colConfigs, dig.Collection)
					if configs == nil {
						dr.logger.Error("No collection config was found for collection", dig.Collection,
							"namespace", dig.Namespace, "txID", dig.TxId)
						continue
					}

					pvtRWSet := dr.extractPvtRWsets(txPvtRWSet.NsPvtRwset, dig.Namespace, dig.Collection)
					if rws.EndorsedAt >= maxEndorsedAt {
						maxEndorsedAt = rws.EndorsedAt
						pvtRWSetWithConfig.CollectionConfig = configs
					}
					pvtRWSetWithConfig.RWSet = append(pvtRWSetWithConfig.RWSet, pvtRWSet...)
				}
			}
			results[common.DigKey{
				Namespace:  dig.Namespace,
				Collection: dig.Collection,
				TxId:       dig.TxId,
				BlockSeq:   dig.BlockSeq,
				SeqInBlock: dig.SeqInBlock,
			}] = pvtRWSetWithConfig
		}

		return results, false, nil
	}
	// Since ledger height is above block sequence number private data is might be available in the ledger
	results, err := dr.fromLedger(digests, blockNum)
	return results, true, err
}
