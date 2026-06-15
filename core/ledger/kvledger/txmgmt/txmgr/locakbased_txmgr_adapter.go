/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/
package txmgr

import (
	"github.com/npci/drunix/core/ledger"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/validation"
)

func (txmgr *LockBasedTxMgr) ValidateAndPrepareLtx(blockAndPvtdata *ledger.BlockAndPvtData, doMVCCValidation bool) (
	[]*validation.AppInitiatedPurgeUpdate, []*validation.LtxStatInfo, []byte, error,
) {
	// Among ValidateAndPrepare(), PrepareForExpiringKeys(), and
	// RemoveStaleAndCommitPvtDataOfOldBlocks(), we can allow only one
	// function to execute at a time. The reason is that each function calls
	// LoadCommittedVersions() which would clear the existing entries in the
	// transient buffer and load new entries (such a transient buffer is not
	// applicable for the golevelDB). As a result, these three functions can
	// interleave and nullify the optimization provided by the bulk read API.
	// Once the ledger cache (FAB-103) is introduced and existing
	// LoadCommittedVersions() is refactored to return a map, we can allow
	// these three functions to execute parallelly.
	logger.Debugf("Waiting for purge mgr to finish the background job of computing expirying keys for the block")
	txmgr.pvtdataPurgeMgr.WaitForPrepareToFinish()
	txmgr.oldBlockCommit.Lock()
	defer txmgr.oldBlockCommit.Unlock()
	logger.Debug("lock acquired on oldBlockCommit for validating read set version against the committed version")

	block := blockAndPvtdata.Block
	logger.Debugf("Validating new block with num trans = [%d]", len(block.Data.Data))
	batch, appPurgeUpdates, txstatsInfo, err := txmgr.commitBatchPreparer.ValidateAndPrepareBatchLtx(blockAndPvtdata, doMVCCValidation)
	if err != nil {
		txmgr.reset()
		return nil, nil, nil, err
	}
	txmgr.currentUpdates = &currentUpdates{block: block, batch: batch}
	if err := txmgr.invokeNamespaceListeners(); err != nil {
		txmgr.reset()
		return nil, nil, nil, err
	}

	updateBytes, err := deterministicBytesForPubAndHashUpdates(batch)
	return appPurgeUpdates, txstatsInfo, updateBytes, err
}

func (txmgr *LockBasedTxMgr) CommitLostLiteBlock(blockAndPvtdata *ledger.BlockAndPvtData) error {
	block := blockAndPvtdata.Block
	logger.Debugf("Constructing updateSet for the lite block %d", block.Header.Number)

	if _, _, _, err := txmgr.ValidateAndPrepareLtx(blockAndPvtdata, false); err != nil {
		return err
	}

	// log every 1000th block at Info level so that statedb rebuild progress can be tracked in production envs.
	if block.Header.Number%1000 == 0 {
		logger.Infof("Recommitting lite block [%d] to state database", block.Header.Number)
	} else {
		logger.Debugf("Recommitting lite block [%d] to state database", block.Header.Number)
	}

	return txmgr.Commit()
}
