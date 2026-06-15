/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/
package txvalidator

import (
	"context"
	"sync"
	"time"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/npci/drunix/internal/pkg/txflags"
	"github.com/npci/drunix/protoutil"
)

/*
DRUNIX:
	VSCC validation flow for lite format transactions.
*/

func (v *TxValidatorPeerAdapter) validateLtx(block *common.Block) error {
	var err error
	var errPos int

	startValidation := time.Now() // timer to log Validate block duration
	logger.Debugf("[%s] START Block Validation for block [%d]", v.ChannelID, block.Header.Number)

	// Initialize trans as valid here, then set invalidation reason code upon invalidation below
	txsfltr := txflags.New(len(block.Data.Data))
	// array of txids
	txidArray := make([]string, len(block.Data.Data))

	results := make(chan *blockValidationResult, len(block.Data.Data))

	wg := &sync.WaitGroup{}
	endorsersTxns := []*common.VsccTransaction{}
	endorsersTxnsLock := &sync.Mutex{}

	wg.Add(len(block.Data.Data))
	go func() {
		for tIdx, d := range block.Data.Data {
			// ensure that we don't have too many concurrent validation workers
			v.Semaphore.Acquire(context.Background())
			go func(index int, data []byte) {
				defer v.Semaphore.Release()
				defer wg.Done()

				env, err := protoutil.GetEnvelopeFromBlock(data)
				if err != nil {
					logger.Warningf("Error getting tx from block: %+v", err)
					results <- &blockValidationResult{
						tIdx:           index,
						validationCode: peer.TxValidationCode_INVALID_OTHER_REASON,
					}
					return
				}

				if env.Type == common.HeaderType_ENDORSER_TRANSACTION {

					erroneousResultEntry := v.checkLtxIdDupsLedger(tIdx, env.LeanEnv.TxId, v.LedgerResources)
					if erroneousResultEntry != nil {
						results <- erroneousResultEntry
						return
					}

					endorsersTxnsLock.Lock()
					endorsersTxns = append(endorsersTxns, &common.VsccTransaction{
						TxIdx: int64(index),
						Env:   env,
					})
					endorsersTxnsLock.Unlock()
					return
				}

				v.validateTx(&blockValidationRequest{
					d:     data,
					block: block,
					tIdx:  index,
				}, results)
			}(tIdx, d)
		}
	}()

	wg.Wait()

	if len(endorsersTxns) > 0 {
		go v.validateTxVsccService(block.Header.Number, endorsersTxns, results)
	}

	logger.Debugf("expecting %d block validation responses", len(block.Data.Data))

	// now we read responses in the order in which they come back
	for i := 0; i < len(block.Data.Data); i++ {
		res := <-results

		if res.err != nil {
			// if there is an error, we buffer its value, wait for
			// all workers to complete validation and then return
			// the error from the first tx in this block that returned an error
			logger.Debugf("got terminal error %s for idx %d", res.err, res.tIdx)

			if err == nil || res.tIdx < errPos {
				err = res.err
				errPos = res.tIdx
			}
		} else {
			// if there was no error, we set the txsfltr and we set the
			// txsChaincodeNames and txsUpgradedChaincodes maps
			logger.Debugf("got result for idx %d, code %d", res.tIdx, res.validationCode)

			txsfltr.SetFlag(res.tIdx, res.validationCode)

			if res.validationCode == peer.TxValidationCode_VALID {
				txidArray[res.tIdx] = res.txid
			}
		}
	}

	// if we're here, all workers have completed the validation.
	// If there was an error we return the error from the first
	// tx in this block that returned an error
	if err != nil {
		return err
	}

	// we mark invalid any transaction that has a txid
	// which is equal to that of a previous tx in this block
	markTXIdDuplicates(txidArray, txsfltr)

	// make sure no transaction has skipped validation
	err = v.allValidated(txsfltr, block)
	if err != nil {
		return err
	}

	// Initialize metadata structure
	protoutil.InitBlockMetadata(block)

	block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER] = txsfltr

	elapsedValidation := time.Since(startValidation) / time.Millisecond // duration in ms
	logger.Infof("[%s] Validated block [%d] in %dms", v.ChannelID, block.Header.Number, elapsedValidation)

	return nil
}

func (v *TxValidator) checkLtxIdDupsLedger(tIdx int, txID string, ldgr LedgerResources) *blockValidationResult {

	// Look for a transaction with the same identifier inside the ledger
	exists, err := ldgr.TxIDExists(txID)
	if err != nil {
		logger.Errorf("Ledger failure while attempting to detect duplicate status for txid %s: %s", txID, err)
		return &blockValidationResult{
			tIdx: tIdx,
			err:  err,
		}
	}
	if exists {
		logger.Error("Duplicate transaction found, ", txID, ", skipping")
		return &blockValidationResult{
			tIdx:           tIdx,
			validationCode: peer.TxValidationCode_DUPLICATE_TXID,
		}
	}
	return nil
}
