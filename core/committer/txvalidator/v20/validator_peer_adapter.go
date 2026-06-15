/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/
package txvalidator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/npci/drunix/bccsp"
	"github.com/npci/drunix/common/policies"
	"github.com/npci/drunix/common/util"
	"github.com/npci/drunix/core/committer/txvalidator/plugin"
	"github.com/npci/drunix/core/committer/txvalidator/v20/plugindispatcher"
	"github.com/npci/drunix/core/committer/txvalidator/v20/vscc_service"
	"github.com/npci/drunix/internal/pkg/txflags"
	"github.com/npci/drunix/protoutil"
	"google.golang.org/grpc/codes"
)

type TxValidatorPeerAdapter struct {
	*TxValidator
	vsccServiceClient *vscc_service.VsccServiceClient
}

func NewTxValidatorPeerAdapter(
	channelID string,
	sem Semaphore,
	cr ChannelResources,
	ler LedgerResources,
	lcr plugindispatcher.LifecycleResources,
	cor plugindispatcher.CollectionResources,
	pm plugin.Mapper,
	channelPolicyManagerGetter policies.ChannelPolicyManagerGetter,
	cryptoProvider bccsp.BCCSP,
	vsccServiceClient *vscc_service.VsccServiceClient,
) *TxValidatorPeerAdapter {
	// Encapsulates interface implementation
	pluginValidator := plugindispatcher.NewPluginValidator(pm, ler, &dynamicDeserializer{cr: cr}, &dynamicCapabilities{cr: cr}, channelPolicyManagerGetter, cor)
	return &TxValidatorPeerAdapter{
		&TxValidator{
			ChannelID:        channelID,
			Semaphore:        sem,
			ChannelResources: cr,
			LedgerResources:  ler,
			Dispatcher:       plugindispatcher.New(channelID, cr, ler, lcr, pluginValidator),
			CryptoProvider:   cryptoProvider,
		}, vsccServiceClient,
	}
}

func (v *TxValidatorPeerAdapter) Validate(block *common.Block) error {

	//DRUNIX : based on channel capabilities decide whether to redirect the validation to vanilla txn formart or to the ltx format validator
	if v.ChannelResources.Capabilities().LeanFormatEnabled() {
		return v.validateLtx(block)
	}
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

				payload, err := protoutil.UnmarshalPayload(env.Payload)
				if err != nil {
					logger.Warningf("Error getting tx from block: %+v", err)
					results <- &blockValidationResult{
						tIdx:           index,
						validationCode: peer.TxValidationCode_INVALID_OTHER_REASON,
					}
					return
				}

				channelHeader, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
				if err != nil {
					logger.Warningf("Error getting tx from block: %+v", err)
					results <- &blockValidationResult{
						tIdx:           index,
						validationCode: peer.TxValidationCode_INVALID_OTHER_REASON,
					}
					return
				}

				if common.HeaderType(channelHeader.Type) == common.HeaderType_ENDORSER_TRANSACTION {

					erroneousResultEntry := v.checkTxIdDupsLedger(tIdx, channelHeader, v.LedgerResources)
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

func (v *TxValidatorPeerAdapter) validateTxVsccService(blockNum uint64, endorsersTxns []*common.VsccTransaction, results chan<- *blockValidationResult) {

	batches := [][]*common.VsccTransaction{}
	dataLen := len(endorsersTxns)

	for i := 0; i < dataLen; i += v.vsccServiceClient.BatchSize {
		end := i + v.vsccServiceClient.BatchSize
		if end > dataLen {
			end = dataLen
		}
		batches = append(batches, endorsersTxns[i:end])
	}

	/*
		DRUNIX
		Added Exponential Retry on failure of vscc service with a timeout of 5 seconds
	*/
	for _, batch := range batches {
		go func(batch []*common.VsccTransaction) {
			err := util.ExponentialBackoffRetry(func() error {

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				res, err := v.vsccServiceClient.Client.ProcessVscc(ctx, &common.VsccRequest{
					BlockNum:     int64(blockNum),
					Transactions: batch,
					ChannelId:    v.ChannelID,
				})
				if err != nil {
					return fmt.Errorf("failed to process vscc request [%v]", err)
				}

				if res.Status != int32(codes.OK) {
					return fmt.Errorf("failed to process vscc request with status code : %v", res.Status)
				}
				for _, validationResponse := range res.ValidationResponses {
					results <- &blockValidationResult{
						tIdx:           int(validationResponse.TxIdx),
						validationCode: peer.TxValidationCode(validationResponse.ValidationCode),
					}
				}
				return nil
			}, 5, 1*time.Second)

			if err != nil {
				for _, txn := range batch {
					results <- &blockValidationResult{
						tIdx: int(txn.TxIdx),
						err:  err,
					}
				}
			}
		}(batch)
	}
}
