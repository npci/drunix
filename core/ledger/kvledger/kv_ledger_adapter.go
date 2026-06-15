/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/
package kvledger

import (
	"time"

	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/npci/drunix/core/ledger"
	"github.com/npci/drunix/core/ledger/internal/version"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/validation"
	"github.com/npci/drunix/core/ledger/pvtdatastorage"
	"github.com/pkg/errors"
)

func (l *kvLedger) CommitLegacyLtx(pvtdataAndBlock *ledger.BlockAndPvtData, commitOpts *ledger.CommitOptions) error {
	blockNumber := pvtdataAndBlock.Block.Header.Number
	l.snapshotMgr.events <- &event{commitStart, blockNumber}
	<-l.snapshotMgr.commitProceed

	if err := l.commitLtx(pvtdataAndBlock, commitOpts); err != nil {
		return err
	}

	l.snapshotMgr.events <- &event{commitDone, blockNumber}
	return nil
}

func (l *kvLedger) commitLtx(pvtdataAndBlock *ledger.BlockAndPvtData, commitOpts *ledger.CommitOptions) error {
	var err error
	block := pvtdataAndBlock.Block
	blockNo := pvtdataAndBlock.Block.Header.Number

	startBlockProcessing := time.Now()
	if commitOpts.FetchPvtDataFromLedger {
		// when we reach here, it means that the pvtdata store has the
		// pvtdata associated with this block but the stateDB might not
		// have it. During the commit of this block, no update would
		// happen in the pvtdata store as it already has the required data.

		// if there is any missing pvtData, reconciler will fetch them
		// and update both the pvtdataStore and stateDB. Hence, we can
		// fetch what is available in the pvtDataStore. If any or
		// all of the pvtdata associated with the block got expired
		// and no longer available in pvtdataStore, eventually these
		// pvtdata would get expired in the stateDB as well (though it
		// would miss the pvtData until then)
		txPvtData, err := l.pvtdataStore.GetPvtDataByBlockNum(blockNo, nil)
		if err != nil {
			return err
		}
		pvtdataAndBlock.PvtData = convertTxPvtDataArrayToMap(txPvtData)
	}

	logger.Debugf("[%s] Validating state for block [%d]", l.ledgerID, blockNo)
	appInitiatedPurgeUpdates, txstatsInfo, updateBatchBytes, err := l.txmgr.ValidateAndPrepareLtx(pvtdataAndBlock, true)
	if err != nil {
		return err
	}
	elapsedBlockProcessing := time.Since(startBlockProcessing)

	startBlockstorageAndPvtdataCommit := time.Now()
	logger.Debugf("[%s] Adding CommitHash to the block [%d]", l.ledgerID, blockNo)
	// we need to ensure that only after a genesis block, commitHash is computed
	// and added to the block. In other words, only after joining a new channel
	// or peer reset, the commitHash would be added to the block
	if block.Header.Number == 1 || len(l.commitHash) != 0 {
		l.addBlockCommitHash(pvtdataAndBlock.Block, updateBatchBytes)
	}

	logger.Debugf("[%s] Committing pvtdata and block [%d] to storage", l.ledgerID, blockNo)
	l.blockAPIsRWLock.Lock()
	defer l.blockAPIsRWLock.Unlock()

	purgeMarkers := []*pvtdatastorage.PurgeMarker{}
	for _, u := range appInitiatedPurgeUpdates {
		purgeMarkers = append(purgeMarkers,
			&pvtdatastorage.PurgeMarker{
				Ns:         u.CompositeKey.Namespace,
				Coll:       u.CompositeKey.CollectionName,
				PvtkeyHash: []byte(u.CompositeKey.KeyHash),
				TxNum:      u.Version.TxNum,
			},
		)
	}

	// retrieve pvtkeys from pvtdata store prior to committing the purge marker, otherwise, the background deletion process
	// may purge hashed index entries before we can fetch the corresponding private keys here.
	pvtKeysToDelete := map[privacyenabledstate.PvtdataCompositeKey]*version.Height{}
	for _, u := range appInitiatedPurgeUpdates {
		pvtKey, err := l.pvtdataStore.FetchPrivateDataRawKey(
			u.CompositeKey.Namespace, u.CompositeKey.CollectionName, []byte(u.CompositeKey.KeyHash),
		)
		if err != nil {
			return err
		}
		if pvtKey == "" {
			continue
		}
		pvtKeysToDelete[privacyenabledstate.PvtdataCompositeKey{
			Namespace:      u.CompositeKey.Namespace,
			CollectionName: u.CompositeKey.CollectionName,
			Key:            pvtKey,
		}] = u.Version
	}

	if err = l.commitToPvtAndBlockStore(pvtdataAndBlock, purgeMarkers); err != nil {
		return err
	}
	elapsedBlockstorageAndPvtdataCommit := time.Since(startBlockstorageAndPvtdataCommit)

	startCommitState := time.Now()
	l.txmgr.UpdateBatchWithAppInitiatedPvtKeysToPurge(pvtKeysToDelete)
	logger.Debugf("[%s] Committing block [%d] transactions to state database", l.ledgerID, blockNo)
	if err = l.txmgr.Commit(); err != nil {
		panic(errors.WithMessage(err, "error during commit to txmgr"))
	}
	elapsedCommitState := time.Since(startCommitState)

	// History database could be written in parallel with state and/or async as a future optimization,
	// although it has not been a bottleneck...no need to clutter the log with elapsed duration.

	// if l.historyDB != nil {
	// 	logger.Debugf("[%s] Committing block [%d] transactions to history database", l.ledgerID, blockNo)
	// 	if err := l.historyDB.Commit(block); err != nil {
	// 		panic(errors.WithMessage(err, "Error during commit to history db"))
	// 	}
	// }

	logger.Infof("[%s] Committed block [%d] with %d transaction(s) in %dms (state_validation=%dms block_and_pvtdata_commit=%dms state_commit=%dms)"+
		" commitHash=[%x]",
		l.ledgerID, block.Header.Number, len(block.Data.Data),
		time.Since(startBlockProcessing)/time.Millisecond,
		elapsedBlockProcessing/time.Millisecond,
		elapsedBlockstorageAndPvtdataCommit/time.Millisecond,
		elapsedCommitState/time.Millisecond,
		l.commitHash,
	)

	l.updateLtxBlockStats(
		elapsedBlockProcessing,
		elapsedBlockstorageAndPvtdataCommit,
		elapsedCommitState,
		txstatsInfo,
	)

	l.sendLiteCommitNotification(blockNo, txstatsInfo)
	return nil
}

func (l *kvLedger) updateLtxBlockStats(
	blockProcessingTime time.Duration,
	blockstorageAndPvtdataCommitTime time.Duration,
	statedbCommitTime time.Duration,
	txstatsInfo []*validation.LtxStatInfo,
) {
	l.stats.updateBlockProcessingTime(blockProcessingTime)
	l.stats.updateBlockstorageAndPvtdataCommitTime(blockstorageAndPvtdataCommitTime)
	l.stats.updateStatedbCommitTime(statedbCommitTime)
	l.stats.updateLiteTransactionsStats(txstatsInfo)
}

func (l *kvLedger) sendLiteCommitNotification(blockNum uint64, txStatsInfo []*validation.LtxStatInfo) {
	l.commitNotifierLock.Lock()
	defer l.commitNotifierLock.Unlock()

	if l.commitNotifier == nil {
		return
	}

	select {
	case <-l.commitNotifier.doneChannel:
		close(l.commitNotifier.dataChannel)
		l.commitNotifier = nil
	default:
		txsByID := map[string]struct{}{}
		txs := []*ledger.CommitNotificationTxInfo{}
		for _, t := range txStatsInfo {
			txID := t.TxIDFromChannelHeader
			_, ok := txsByID[txID]

			if txID == "" || ok {
				continue
			}
			txsByID[txID] = struct{}{}

			txs = append(txs, &ledger.CommitNotificationTxInfo{
				TxType:         t.TxType,
				TxID:           t.TxIDFromChannelHeader,
				ValidationCode: t.ValidationCode,
				ChaincodeID: &peer.ChaincodeID{
					Path:    t.ChaincodeID.Path,
					Name:    t.ChaincodeID.Name,
					Version: t.ChaincodeID.Version,
				},
				ChaincodeEventData: t.ChaincodeEventData,
			})
		}

		l.commitNotifier.dataChannel <- &ledger.CommitNotification{
			BlockNumber: blockNum,
			TxsInfo:     txs,
		}
	}
}
