/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/

package privdata

import (
	"bytes"
	"sync"
	"time"

	"github.com/hyperledger/fabric-protos-go/common"
	vsccErrors "github.com/npci/drunix/common/errors"
	commonutil "github.com/npci/drunix/common/util"
	pvtdatasc "github.com/npci/drunix/core/common/privdata"
	"github.com/npci/drunix/core/ledger"
	pvtdatacommon "github.com/npci/drunix/gossip/privdata/common"
	"github.com/npci/drunix/gossip/util"
)

/*
DRUNIX:
	methods to fetch transient data for lite transactions
*/

type RetrievedLtxPvtdata struct {
	RetrievedPvtdata
	pvtdataRetrievalInfo *ltxPvtdataRetrievalInfo
}

func (r *RetrievedLtxPvtdata) Purge() {

	purgeStart := time.Now()
	if r.transientStore.KVStore != nil {
		r.PurgeKVStore()
	}

	if len(r.blockPvtdata.PvtData) > 0 {
		// Finally, purge all transactions in block - valid or not valid.
		if err := r.transientStore.PurgeByTxids(r.pvtdataRetrievalInfo.txns); err != nil {
			r.logger.Errorf("Purging transactions %v failed: %s", r.pvtdataRetrievalInfo.txns, err)
		}
	}

	blockNum := r.blockNum
	if blockNum%r.transientBlockRetention == 0 && blockNum > r.transientBlockRetention {
		err := r.transientStore.PurgeBelowHeight(blockNum - r.transientBlockRetention)
		if err != nil {
			r.logger.Errorf("Failed purging data from transient store at block [%d]: %s", blockNum, err)
		}
	}

	r.purgeDurationHistogram.Observe(time.Since(purgeStart).Seconds())
}

type ltxPvtdataRetrievalInfo struct {
	sources                      map[rwSetKey][]*common.Endorsement
	expectedHashes               map[rwSetKey][]byte
	txns                         []string
	remainingEligibleMissingKeys rwsetKeys
	resolvedAsToReconcileLater   rwsetKeys
	ineligibleMissingKeys        rwsetKeys
}

// populateFromTransientStore populates pvtdata with data fetched from transient store
// and updates pvtdataRetrievalInfo by removing missing data that was fetched from transient store
func (pdp *PvtdataProvider) populateFromKVTransientStore(pvtdata rwsetByKeys, pvtdataRetrievalInfo *pvtdataRetrievalInfo) {
	pdp.logger.Debugf("Attempting to retrieve %d private write sets from transient store.", len(pvtdataRetrievalInfo.remainingEligibleMissingKeys))

	lock := &sync.RWMutex{}
	wg := &sync.WaitGroup{}
	iterMap := rwsetKeys{}

	rwsetKeysMap := make(map[string][]ledger.PvtNsCollFilter)
	for k, v := range pvtdataRetrievalInfo.remainingEligibleMissingKeys {
		iterMap[k] = v
		filter := ledger.NewPvtNsCollFilter()
		filter.Add(k.namespace, k.collection)
		if len(rwsetKeysMap[k.txID]) == 0 {
			rwsetKeysMap[k.txID] = []ledger.PvtNsCollFilter{filter}
		} else {
			rwsetKeysMap[k.txID] = append(rwsetKeysMap[k.txID], filter)
		}
	}

	transientstoreData, err := pdp.transientStore.KVStore.GetTxPvtRWSetByTxidV2(rwsetKeysMap)
	if err != nil {
		return
	}

	for rwKey := range iterMap {
		wg.Add(1)
		go func(lock *sync.RWMutex, k rwSetKey) {
			defer wg.Done()
			for _, resArr := range transientstoreData[k.txID] {
				for _, res := range resArr {

					if res == nil {
						continue
					}
					if res.PvtSimulationResultsWithConfig == nil {
						pdp.logger.Warningf("Resultset's PvtSimulationResultsWithConfig for txID [%s] is nil. Skipping.", k.txID)
						continue
					}
					simRes := res.PvtSimulationResultsWithConfig
					// simRes.PvtRwset will be nil if the transient store contains an entry for the txid but the entry does not contain the data for the collection
					if simRes.PvtRwset == nil {
						pdp.logger.Debugf("The PvtRwset of PvtSimulationResultsWithConfig for txID [%s] is nil. Skipping.", k.txID)
						continue
					}
					for _, ns := range simRes.PvtRwset.NsPvtRwset {
						for _, col := range ns.CollectionPvtRwset {
							key := rwSetKey{
								txID:       k.txID,
								seqInBlock: k.seqInBlock,
								collection: col.CollectionName,
								namespace:  ns.Namespace,
							}

							// skip if not missing
							lock.RLock()
							if _, missing := pvtdataRetrievalInfo.remainingEligibleMissingKeys[key]; !missing {
								lock.RUnlock()
								continue
							}
							lock.RUnlock()
							// populate the pvtdata with the RW set from the transient store
							pdp.logger.Debugf("Found private data for key %v in transient store", key)
							lock.Lock()
							pvtdata[key] = col.Rwset
							// remove key from missing

							delete(pvtdataRetrievalInfo.remainingEligibleMissingKeys, key)
							lock.Unlock()
						} // iterating over all collections
					} // iterating over all namespaces
				} // iterating over the TxPvtRWSet results
			}
		}(lock, rwKey)

	}
	wg.Wait()
}

func (pdp *PvtdataProvider) populateLtxFromKVTransientStore(pvtdata rwsetByKeys, pvtdataRetrievalInfo *ltxPvtdataRetrievalInfo) {
	pdp.logger.Debugf("Attempting to retrieve %d private write sets from transient store.", len(pvtdataRetrievalInfo.remainingEligibleMissingKeys))

	lock := &sync.RWMutex{}
	wg := &sync.WaitGroup{}
	iterMap := rwsetKeys{}

	rwsetKeysMap := make(map[string][]ledger.PvtNsCollFilter)
	for k, v := range pvtdataRetrievalInfo.remainingEligibleMissingKeys {
		iterMap[k] = v
		filter := ledger.NewPvtNsCollFilter()
		filter.Add(k.namespace, k.collection)
		if len(rwsetKeysMap[k.txID]) == 0 {
			rwsetKeysMap[k.txID] = []ledger.PvtNsCollFilter{filter}
		} else {
			rwsetKeysMap[k.txID] = append(rwsetKeysMap[k.txID], filter)
		}
	}

	transientstoreData, err := pdp.transientStore.KVStore.GetTxPvtRWSetByTxidV2(rwsetKeysMap)
	if err != nil {
		return
	}

	for rwKey := range iterMap {
		wg.Add(1)
		go func(lock *sync.RWMutex, k rwSetKey) {
			defer wg.Done()
			for _, resArr := range transientstoreData[k.txID] {
				for _, res := range resArr {

					if res == nil {
						continue
					}
					if res.PvtSimulationResultsWithConfig == nil {
						pdp.logger.Warningf("Resultset's PvtSimulationResultsWithConfig for txID [%s] is nil. Skipping.", k.txID)
						continue
					}
					simRes := res.PvtSimulationResultsWithConfig
					// simRes.PvtRwset will be nil if the transient store contains an entry for the txid but the entry does not contain the data for the collection
					if simRes.PvtRwset == nil {
						pdp.logger.Debugf("The PvtRwset of PvtSimulationResultsWithConfig for txID [%s] is nil. Skipping.", k.txID)
						continue
					}
					for _, ns := range simRes.PvtRwset.NsPvtRwset {
						for _, col := range ns.CollectionPvtRwset {
							key := rwSetKey{
								txID:       k.txID,
								seqInBlock: k.seqInBlock,
								collection: col.CollectionName,
								namespace:  ns.Namespace,
							}

							// skip if not missing
							lock.RLock()
							if _, missing := pvtdataRetrievalInfo.remainingEligibleMissingKeys[key]; !missing {
								lock.RUnlock()
								continue
							}
							lock.RUnlock()
							// populate the pvtdata with the RW set from the transient store
							pdp.logger.Debugf("Found private data for key %v in transient store", key)
							lock.Lock()
							pvtdata[key] = col.Rwset
							// remove key from missing

							delete(pvtdataRetrievalInfo.remainingEligibleMissingKeys, key)
							lock.Unlock()
						} // iterating over all collections
					} // iterating over all namespaces
				} // iterating over the TxPvtRWSet results
			}
		}(lock, rwKey)

	}
	wg.Wait()
}

func (r *RetrievedPvtdata) PurgeKVStore() {
	blockNum := r.blockNum

	if len(r.blockPvtdata.PvtData) > 0 {
		// Finally, purge all transactions in block - valid or not valid.
		if err := r.transientStore.KVStore.PurgeByTxids(r.pvtdataRetrievalInfo.txns); err != nil {
			r.logger.Errorf("Purging transactions %v failed: %s", r.pvtdataRetrievalInfo.txns, err)
		}
	}

	if blockNum > r.transientBlockRetention {
		// purge pvt data below retention height
		err := r.transientStore.KVStore.PurgeTxIdsByHeight(blockNum - r.transientBlockRetention)
		if err != nil {
			r.logger.Errorf("Failed purging data from transient store at block [%d]: %s", blockNum, err)
		}
	}
}

func (pdp *PvtdataProvider) RetrieveLtxPvtdata(pvtdataToRetrieve []*ledger.LtxPvtdataInfo) (*RetrievedLtxPvtdata, error) {
	retrievedPvtdata := &RetrievedLtxPvtdata{
		RetrievedPvtdata: RetrievedPvtdata{
			transientStore:          pdp.transientStore,
			logger:                  pdp.logger,
			purgeDurationHistogram:  pdp.purgeDurationHistogram,
			blockNum:                pdp.blockNum,
			transientBlockRetention: pdp.transientBlockRetention,
		},
	}

	listMissingStart := time.Now()
	eligibilityComputer := &eligibilityComputer{
		logger:                  pdp.logger,
		storePvtdataOfInvalidTx: pdp.storePvtdataOfInvalidTx,
		channelID:               pdp.channelID,
		selfSignedData:          pdp.selfSignedData,
		idDeserializerFactory:   pdp.idDeserializerFactory,
	}

	pvtdataRetrievalInfo, err := eligibilityComputer.computeLtxEligibility(pdp.mspID, pvtdataToRetrieve)
	if err != nil {
		return nil, err
	}
	pdp.listMissingPrivateDataDurationHistogram.Observe(time.Since(listMissingStart).Seconds())

	pvtdata := make(rwsetByKeys)

	// If there is no private data to retrieve for the block, skip all population attempts and return
	if len(pvtdataRetrievalInfo.remainingEligibleMissingKeys) == 0 {
		pdp.logger.Debugf("No eligible collection private write sets to fetch for block [%d]", pdp.blockNum)
		retrievedPvtdata.pvtdataRetrievalInfo = pvtdataRetrievalInfo
		retrievedPvtdata.blockPvtdata = pdp.prepareBlockLtxPvtdata(pvtdata, pvtdataRetrievalInfo)
		return retrievedPvtdata, nil
	}

	fetchStats := &fetchStats{}

	totalEligibleMissingKeysToRetrieve := len(pvtdataRetrievalInfo.remainingEligibleMissingKeys)
	/*
		// POPULATE FROM CACHE
		pdp.populateFromCache(pvtdata, pvtdataRetrievalInfo, pvtdataToRetrieve)
		fetchStats.fromLocalCache = totalEligibleMissingKeysToRetrieve - len(pvtdataRetrievalInfo.remainingEligibleMissingKeys)

		if len(pvtdataRetrievalInfo.remainingEligibleMissingKeys) == 0 {
			pdp.logger.Infof(util.ColorGreen("Successfully fetched (or marked to reconcile later) all %d eligible collection private write sets for block [%d] %s"), totalEligibleMissingKeysToRetrieve, pdp.blockNum, fetchStats)
			retrievedPvtdata.pvtdataRetrievalInfo = pvtdataRetrievalInfo
			retrievedPvtdata.blockPvtdata = pdp.prepareBlockPvtdata(pvtdata, pvtdataRetrievalInfo)
			return retrievedPvtdata, nil
		}
	*/

	// POPULATE FROM TRANSIENT STORE
	numRemainingToFetch := len(pvtdataRetrievalInfo.remainingEligibleMissingKeys)
	if pdp.transientStore.KVStore != nil {
		// DRUNIX: using kvstore
		pdp.populateLtxFromKVTransientStore(pvtdata, pvtdataRetrievalInfo)
	} else {
		pdp.populateLtxFromTransientStore(pvtdata, pvtdataRetrievalInfo)
	}
	fetchStats.fromTransientStore = numRemainingToFetch - len(pvtdataRetrievalInfo.remainingEligibleMissingKeys)

	if len(pvtdataRetrievalInfo.remainingEligibleMissingKeys) == 0 {
		pdp.logger.Infof(util.ColorGreen("Successfully fetched (or marked to reconcile later) all %d eligible collection private write sets for block [%d] %s"), totalEligibleMissingKeysToRetrieve, pdp.blockNum, fetchStats)
		retrievedPvtdata.pvtdataRetrievalInfo = pvtdataRetrievalInfo
		retrievedPvtdata.blockPvtdata = pdp.prepareBlockLtxPvtdata(pvtdata, pvtdataRetrievalInfo)
		return retrievedPvtdata, nil
	}

	// POPULATE FROM REMOTE PEERS
	numRemainingToFetch = len(pvtdataRetrievalInfo.remainingEligibleMissingKeys)
	retryThresh := pdp.pullRetryThreshold
	pdp.logger.Debugf("Could not find all collection private write sets in local peer transient store for block [%d]", pdp.blockNum)
	pdp.logger.Debugf("Fetching %d collection private write sets from remote peers for a maximum duration of %s", len(pvtdataRetrievalInfo.remainingEligibleMissingKeys), retryThresh)
	startPull := time.Now()
	for len(pvtdataRetrievalInfo.remainingEligibleMissingKeys) > 0 && time.Since(startPull) < retryThresh {
		if needToRetry := pdp.populateLtxFromRemotePeers(pvtdata, pvtdataRetrievalInfo); !needToRetry {
			break
		}
		// If there are still missing keys, sleep before retry
		pdp.sleeper.Sleep(pullRetrySleepInterval)
	}
	elapsedPull := int64(time.Since(startPull) / time.Millisecond) // duration in ms
	pdp.fetchDurationHistogram.Observe(time.Since(startPull).Seconds())

	fetchStats.fromRemotePeer = numRemainingToFetch - len(pvtdataRetrievalInfo.remainingEligibleMissingKeys)

	if len(pvtdataRetrievalInfo.remainingEligibleMissingKeys) == 0 {
		pdp.logger.Debugf("Fetched (or marked to reconcile later) collection private write sets from remote peers for block [%d] (%dms)", pdp.blockNum, elapsedPull)
		pdp.logger.Infof(util.ColorGreen("Successfully fetched (or marked to reconcile later) all %d eligible collection private write sets for block [%d] %s"), totalEligibleMissingKeysToRetrieve, pdp.blockNum, fetchStats)
	} else {
		pdp.logger.Warningf("Could not fetch (or mark to reconcile later) %d eligible collection private write sets for block [%d] %s. Will commit block with missing private write sets:[%v]",
			totalEligibleMissingKeysToRetrieve, pdp.blockNum, fetchStats, pvtdataRetrievalInfo.remainingEligibleMissingKeys)
	}

	retrievedPvtdata.pvtdataRetrievalInfo = pvtdataRetrievalInfo
	retrievedPvtdata.blockPvtdata = pdp.prepareBlockLtxPvtdata(pvtdata, pvtdataRetrievalInfo)
	return retrievedPvtdata, nil
}

func (ec *eligibilityComputer) computeLtxEligibility(mspID string, pvtdataToRetrieve []*ledger.LtxPvtdataInfo) (*ltxPvtdataRetrievalInfo, error) {
	sources := make(map[rwSetKey][]*common.Endorsement)
	expectedHashes := make(map[rwSetKey][]byte)
	eligibleMissingKeys := make(rwsetKeys)
	ineligibleMissingKeys := make(rwsetKeys)

	var txList []string
	for _, txPvtdata := range pvtdataToRetrieve {
		txID := txPvtdata.TxID
		seqInBlock := txPvtdata.SeqInBlock
		invalid := txPvtdata.Invalid
		txList = append(txList, txID)
		if invalid && !ec.storePvtdataOfInvalidTx {
			ec.logger.Debugf("Skipping Tx [%s] at sequence [%d] because it's invalid.", txID, seqInBlock)
			continue
		}
		deserializer := ec.idDeserializerFactory.GetIdentityDeserializer(ec.channelID)
		for _, colInfo := range txPvtdata.CollectionPvtdataInfo {
			ns := colInfo.Namespace
			col := colInfo.Collection
			hash := colInfo.ExpectedHash
			endorsers := colInfo.Endorsers
			colConfig := colInfo.CollectionConfig

			policy, err := pvtdatasc.NewSimpleCollection(colConfig, deserializer)
			if err != nil {
				ec.logger.Errorf("Failed to retrieve collection access policy for chaincode [%s], collection name [%s] for txID [%s]: %s.",
					ns, col, txID, err)
				return nil, &vsccErrors.VSCCExecutionFailureError{Err: err}
			}

			key := rwSetKey{
				txID:       txID,
				seqInBlock: seqInBlock,
				namespace:  ns,
				collection: col,
			}

			// First check if mspID is found in the MemberOrgs before falling back to AccessFilter policy evaluation
			memberOrgs := policy.MemberOrgs()
			if _, ok := memberOrgs[mspID]; !ok &&
				!policy.AccessFilter()(ec.selfSignedData) {
				ec.logger.Debugf("Peer is not eligible for collection: chaincode [%s], "+
					"collection name [%s], txID [%s] the policy is [%#v]. Skipping.",
					ns, col, txID, policy)
				ineligibleMissingKeys[key] = rwsetInfo{}
				continue
			}

			// treat all eligible keys as missing
			eligibleMissingKeys[key] = rwsetInfo{
				invalid: invalid,
			}
			sources[key] = ltxEndorsersFromEligibleOrgs(ns, col, endorsers, memberOrgs)
			expectedHashes[key] = hash
		}
	}

	return &ltxPvtdataRetrievalInfo{
		sources:                      sources,
		expectedHashes:               expectedHashes,
		txns:                         txList,
		remainingEligibleMissingKeys: eligibleMissingKeys,
		resolvedAsToReconcileLater:   make(rwsetKeys),
		ineligibleMissingKeys:        ineligibleMissingKeys,
	}, nil
}

func ltxEndorsersFromEligibleOrgs(ns string, col string, endorsers []*common.Endorsement, orgs map[string]struct{}) []*common.Endorsement {
	var res []*common.Endorsement
	for _, e := range endorsers {
		if _, ok := orgs[e.Endorser.Mspid]; !ok {
			logger.Debug(e.Endorser.Mspid, "isn't among the collection's orgs:", orgs, "for namespace", ns, ",collection", col)
			continue
		}
		res = append(res, e)
	}
	return res
}

func (pdp *PvtdataProvider) prepareBlockLtxPvtdata(pvtdata rwsetByKeys, pvtdataRetrievalInfo *ltxPvtdataRetrievalInfo) *ledger.BlockPvtdata {
	blockPvtdata := &ledger.BlockPvtdata{
		PvtData:        make(ledger.TxPvtDataMap),
		MissingPvtData: make(ledger.TxMissingPvtData),
	}

	for seqInBlock, nsRWS := range pvtdata.bySeqsInBlock() {
		// add all found pvtdata to blockPvtDataPvtdata for seqInBlock
		blockPvtdata.PvtData[seqInBlock] = &ledger.TxPvtData{
			SeqInBlock: seqInBlock,
			WriteSet:   nsRWS.toRWSet(),
		}
	}

	for key := range pvtdataRetrievalInfo.resolvedAsToReconcileLater {
		blockPvtdata.MissingPvtData.Add(key.seqInBlock, key.namespace, key.collection, true)
	}

	for key := range pvtdataRetrievalInfo.remainingEligibleMissingKeys {
		blockPvtdata.MissingPvtData.Add(key.seqInBlock, key.namespace, key.collection, true)
	}

	for key := range pvtdataRetrievalInfo.ineligibleMissingKeys {
		blockPvtdata.MissingPvtData.Add(key.seqInBlock, key.namespace, key.collection, false)
	}

	return blockPvtdata
}

func (pdp *PvtdataProvider) populateLtxFromRemotePeers(pvtdata rwsetByKeys, pvtdataRetrievalInfo *ltxPvtdataRetrievalInfo) bool {
	pdp.logger.Debugf("Attempting to retrieve %d private write sets from remote peers.", len(pvtdataRetrievalInfo.remainingEligibleMissingKeys))

	dig2src := make(map[pvtdatacommon.DigKey][]*common.Endorsement)
	var skipped int
	for k, v := range pvtdataRetrievalInfo.remainingEligibleMissingKeys {
		if v.invalid && pdp.skipPullingInvalidTransactions {
			pdp.logger.Debugf("Skipping invalid key [%v] because peer is configured to skip pulling rwsets of invalid transactions.", k)
			skipped++
			continue
		}
		pdp.logger.Debugf("Fetching [%v] from remote peers", k)
		dig := pvtdatacommon.DigKey{
			TxId:       k.txID,
			SeqInBlock: k.seqInBlock,
			Collection: k.collection,
			Namespace:  k.namespace,
			BlockSeq:   pdp.blockNum,
		}
		dig2src[dig] = pvtdataRetrievalInfo.sources[k]
	}

	if len(dig2src) == 0 {
		return false
	}

	fetchedData, err := pdp.fetcher.ltxFetch(dig2src)
	if err != nil {
		pdp.logger.Warningf("Failed fetching private data from remote peers for dig2src:[%v], err: %s", dig2src, err)
		return true
	}

	// Iterate over data fetched from remote peers
	for _, element := range fetchedData.AvailableElements {
		dig := element.Digest
		for _, rws := range element.Payload {
			key := rwSetKey{
				txID:       dig.TxId,
				namespace:  dig.Namespace,
				collection: dig.Collection,
				seqInBlock: dig.SeqInBlock,
			}
			// skip if not missing
			if _, missing := pvtdataRetrievalInfo.remainingEligibleMissingKeys[key]; !missing {
				// key isn't missing and was never fetched earlier, log that it wasn't originally requested
				if _, exists := pvtdata[key]; !exists {
					pdp.logger.Debugf("Ignoring [%v] because it was never requested.", key)
				}
				continue
			}

			if bytes.Equal(pvtdataRetrievalInfo.expectedHashes[key], commonutil.ComputeSHA256(rws)) {
				// populate the pvtdata with the RW set from the remote peer
				pvtdata[key] = rws
			} else {
				// the private data was fetched from the remote peer but the hash of writeset did not match with what is present in block.
				// Most likely scenarios for this are when either the sending peer is bootstrapped from a snapshot or it has purged some
				// of the keys from the private data, based on a user initiated transaction. In this case, we treat this as missing data,
				// that would be tried later via reconciliation
				pvtdataRetrievalInfo.resolvedAsToReconcileLater[key] = pvtdataRetrievalInfo.remainingEligibleMissingKeys[key]
			}
			// remove key from missing
			delete(pvtdataRetrievalInfo.remainingEligibleMissingKeys, key)
			pdp.logger.Debugf("Fetched [%v]", key)
		}
	}
	// Iterate over purged data
	for _, dig := range fetchedData.PurgedElements {
		// delete purged key from missing keys
		for missingPvtRWKey := range pvtdataRetrievalInfo.remainingEligibleMissingKeys {
			if missingPvtRWKey.namespace == dig.Namespace &&
				missingPvtRWKey.collection == dig.Collection &&
				missingPvtRWKey.seqInBlock == dig.SeqInBlock &&
				missingPvtRWKey.txID == dig.TxId {
				delete(pvtdataRetrievalInfo.remainingEligibleMissingKeys, missingPvtRWKey)
				pdp.logger.Warningf("Missing key because was purged or will soon be purged, "+
					"continue block commit without [%+v] in private rwset", missingPvtRWKey)
			}
		}
	}

	return len(pvtdataRetrievalInfo.remainingEligibleMissingKeys) > skipped
}

func (pdp *PvtdataProvider) populateLtxFromTransientStore(pvtdata rwsetByKeys, pvtdataRetrievalInfo *ltxPvtdataRetrievalInfo) {
	pdp.logger.Debugf("Attempting to retrieve %d private write sets from transient store.", len(pvtdataRetrievalInfo.remainingEligibleMissingKeys))

	// Put into pvtdata RW sets that are missing and found in the transient store
	for k := range pvtdataRetrievalInfo.remainingEligibleMissingKeys {
		filter := ledger.NewPvtNsCollFilter()
		filter.Add(k.namespace, k.collection)
		iterator, err := pdp.transientStore.GetTxPvtRWSetByTxid(k.txID, filter)
		if err != nil {
			pdp.logger.Warningf("Failed fetching private data from transient store: Failed obtaining iterator from transient store: %s", err)
			return
		}
		defer iterator.Close()
		for {
			res, err := iterator.Next()
			if err != nil {
				pdp.logger.Warningf("Failed fetching private data from transient store: Failed iterating over transient store data: %s", err)
				return
			}
			if res == nil {
				// End of iteration
				break
			}
			if res.PvtSimulationResultsWithConfig == nil {
				pdp.logger.Warningf("Resultset's PvtSimulationResultsWithConfig for txID [%s] is nil. Skipping.", k.txID)
				continue
			}
			simRes := res.PvtSimulationResultsWithConfig
			// simRes.PvtRwset will be nil if the transient store contains an entry for the txid but the entry does not contain the data for the collection
			if simRes.PvtRwset == nil {
				pdp.logger.Debugf("The PvtRwset of PvtSimulationResultsWithConfig for txID [%s] is nil. Skipping.", k.txID)
				continue
			}
			for _, ns := range simRes.PvtRwset.NsPvtRwset {
				for _, col := range ns.CollectionPvtRwset {
					key := rwSetKey{
						txID:       k.txID,
						seqInBlock: k.seqInBlock,
						collection: col.CollectionName,
						namespace:  ns.Namespace,
					}
					// skip if not missing
					if _, missing := pvtdataRetrievalInfo.remainingEligibleMissingKeys[key]; !missing {
						continue
					}

					if !bytes.Equal(pvtdataRetrievalInfo.expectedHashes[key], commonutil.ComputeSHA256(col.Rwset)) {
						continue
					}
					// populate the pvtdata with the RW set from the transient store
					pdp.logger.Debugf("Found private data for key %v in transient store", key)
					pvtdata[key] = col.Rwset
					// remove key from missing
					delete(pvtdataRetrievalInfo.remainingEligibleMissingKeys, key)
				} // iterating over all collections
			} // iterating over all namespaces
		} // iterating over the TxPvtRWSet results
	}
}
