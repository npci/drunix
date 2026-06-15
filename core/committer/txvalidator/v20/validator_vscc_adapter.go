/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/
package txvalidator

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/npci/drunix/bccsp"
	"github.com/npci/drunix/common/configtx"
	commonerrors "github.com/npci/drunix/common/errors"
	"github.com/npci/drunix/common/metrics"
	"github.com/npci/drunix/common/policies"
	"github.com/npci/drunix/core/committer/txvalidator/plugin"
	"github.com/npci/drunix/core/committer/txvalidator/v20/plugindispatcher"
	"github.com/npci/drunix/core/common/validation"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/statedb"
	"github.com/npci/drunix/protoutil"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
)

type DispatcherAdapter interface {
	// Dispatch invokes the appropriate validation plugin for the supplied transaction in the block
	Dispatch(seq int, payload *common.Payload, env *common.Envelope, blockNum uint64) (peer.TxValidationCode, error)
	DispatchLtx(seq int, payload *common.Envelope, blockNum uint64) (peer.TxValidationCode, error)
}

type TxValidatorVSCCAdapter struct {
	TxValidator
	DispatcherAdapter DispatcherAdapter
}

func NewTxValidatorVSCCAdapter(
	channelID string,
	sem Semaphore,
	cr ChannelResources,
	pm plugin.MapperAdapter,
	channelPolicyManagerGetter policies.ChannelPolicyManagerGetter,
	cryptoProvider bccsp.BCCSP,
	peerEndpoint string,
	dbProvider statedb.VersionedDB,
	cor plugindispatcher.CollectionResources,
	lcr plugindispatcher.LifecycleResources,
	metricsProvider metrics.Provider,
) (*TxValidatorVSCCAdapter, error) {

	qec := plugindispatcher.NewCustomQueryExecuterCreator(dbProvider)

	// Encapsulates interface implementation
	pluginValidator := plugindispatcher.NewPluginValidatorAdapter(pm, qec, &dynamicDeserializer{cr: cr}, &dynamicCapabilities{cr: cr}, channelPolicyManagerGetter, cor)
	txValidator := &TxValidatorVSCCAdapter{
		TxValidator: TxValidator{
			ChannelID: channelID,
			Semaphore: sem,
			// Dispatcher:       plugindispatcher.NewAdapter(channelID, nil, qec, lcr, pluginValidator),
			CryptoProvider:   cryptoProvider,
			ChannelResources: cr,
		},
		DispatcherAdapter: plugindispatcher.NewAdapter(channelID, nil, qec, lcr, pluginValidator),
	}
	return txValidator, nil
}

type blockValidationRequestAdapter struct {
	blockNum uint64
	tIdx     int
	env      *common.Envelope
}

func (v *TxValidatorVSCCAdapter) validateTx(req *blockValidationRequestAdapter, results chan<- *blockValidationResult) {
	blockNum := req.blockNum
	env := req.env
	tIdx := req.tIdx
	txID := ""

	if env != nil {
		// validate the transaction: here we check that the transaction
		// is properly formed, properly signed and that the security
		// chain binding proposal to endorsements to tx holds. We do
		// NOT check the validity of endorsements, though. That's a
		// job for the validation plugins
		logger.Debugf("[%s] validateTx starts for block %d env %p txn %d", v.ChannelID, blockNum, env, tIdx)
		defer logger.Debugf("[%s] validateTx completes for block %d env %p txn %d", v.ChannelID, blockNum, env, tIdx)
		var payload *common.Payload
		var err error
		var txResult peer.TxValidationCode

		// function call here!!!
		if payload, txResult = validation.ValidateTransaction(env, v.CryptoProvider); txResult != peer.TxValidationCode_VALID {
			logger.Errorf("Invalid transaction with index %d", tIdx)
			results <- &blockValidationResult{
				tIdx:           tIdx,
				validationCode: txResult,
			}
			return
		}

		chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
		if err != nil {
			logger.Warningf("Could not unmarshal channel header, err %s, skipping", err)
			results <- &blockValidationResult{
				tIdx:           tIdx,
				validationCode: peer.TxValidationCode_INVALID_OTHER_REASON,
			}
			return
		}

		channel := chdr.ChannelId
		logger.Debugf("Transaction is for channel %s", channel)

		if !v.chainExists(channel) {
			logger.Errorf("Dropping transaction for non-existent channel %s", channel)
			results <- &blockValidationResult{
				tIdx:           tIdx,
				validationCode: peer.TxValidationCode_TARGET_CHAIN_NOT_FOUND,
			}
			return
		}

		if common.HeaderType(chdr.Type) == common.HeaderType_ENDORSER_TRANSACTION {

			txID = chdr.TxId

			// Check duplicate transactions
			// erroneousResultEntry := v.checkTxIdDupsLedger(tIdx, chdr, v.LedgerResources)
			// if erroneousResultEntry != nil {
			// 	results <- erroneousResultEntry
			// 	return
			// }

			// Validate tx with plugins
			logger.Debug("Validating transaction with plugins")

			cde, err := v.DispatcherAdapter.Dispatch(tIdx, payload, env, blockNum)
			if err != nil {
				logger.Errorf("Dispatch for transaction txId = %s returned error: %s", txID, err)
				switch err.(type) {
				case *commonerrors.VSCCExecutionFailureError:
					results <- &blockValidationResult{
						tIdx: tIdx,
						err:  err,
					}
					return
				case *commonerrors.VSCCInfoLookupFailureError:
					results <- &blockValidationResult{
						tIdx: tIdx,
						err:  err,
					}
					return
				default:
					results <- &blockValidationResult{
						tIdx:           tIdx,
						validationCode: cde,
					}
					return
				}
			}
		} else if common.HeaderType(chdr.Type) == common.HeaderType_CONFIG {
			configEnvelope, err := configtx.UnmarshalConfigEnvelope(payload.Data)
			if err != nil {
				err = errors.WithMessage(err, "error unmarshalling config which passed initial validity checks")
				logger.Criticalf("%+v", err)
				results <- &blockValidationResult{
					tIdx: tIdx,
					err:  err,
				}
				return
			}

			logger.Debugw("Config transaction envelope passed validation checks", "channel", channel)
			if err := v.ChannelResources.Apply(configEnvelope); err != nil {
				err = errors.WithMessage(err, "error validating config which passed initial validity checks")
				logger.Criticalf("%+v", err)
				results <- &blockValidationResult{
					tIdx: tIdx,
					err:  err,
				}
				return
			}
			logger.Infow("Config transaction validated and applied to channel resources", "channel", channel)
		} else {
			logger.Warningf("Unknown transaction type [%s] in block number [%d] transaction index [%d]",
				common.HeaderType(chdr.Type), blockNum, tIdx)
			results <- &blockValidationResult{
				tIdx:           tIdx,
				validationCode: peer.TxValidationCode_UNKNOWN_TX_TYPE,
			}
			return
		}

		if _, err := proto.Marshal(env); err != nil {
			logger.Warningf("Cannot marshal transaction: %s", err)
			results <- &blockValidationResult{
				tIdx:           tIdx,
				validationCode: peer.TxValidationCode_MARSHAL_TX_ERROR,
			}
			return
		}
		// Succeeded to pass down here, transaction is valid
		results <- &blockValidationResult{
			tIdx:           tIdx,
			validationCode: peer.TxValidationCode_VALID,
			txid:           txID,
		}
		return
	} else {
		logger.Warning("Nil tx from block")
		results <- &blockValidationResult{
			tIdx:           tIdx,
			validationCode: peer.TxValidationCode_NIL_ENVELOPE,
		}
		return
	}
}

func (v *TxValidatorVSCCAdapter) validateLtxTxn(req *blockValidationRequestAdapter, results chan<- *blockValidationResult) {
	blockNum := req.blockNum
	env := req.env
	tIdx := req.tIdx
	txID := ""

	if env != nil {
		// validate the transaction: here we check that the transaction
		// is properly formed, properly signed and that the security
		// chain binding proposal to endorsements to tx holds. We do
		// NOT check the validity of endorsements, though. That's a
		// job for the validation plugins
		logger.Debugf("[%s] validateTx starts for block %d env %p txn %d", v.ChannelID, blockNum, env, tIdx)
		defer logger.Debugf("[%s] validateTx completes for block %d env %p txn %d", v.ChannelID, blockNum, env, tIdx)
		var err error
		var txResult peer.TxValidationCode
		var channel string
		var txType common.HeaderType
		// function call here!!!    --*--
		if txResult = validation.ValidateLtxTransaction(env, v.CryptoProvider); txResult != peer.TxValidationCode_VALID {
			logger.Errorf("Invalid transaction with index %d", tIdx)
			results <- &blockValidationResult{
				tIdx:           tIdx,
				validationCode: txResult,
			}
			return
		}

		payload, chdr, _, err := protoutil.GetChannelAndSignatureHeader(env)
		if err != nil {
			logger.Warningf("Could not unmarshal channel header, err %s, skipping", err)
			results <- &blockValidationResult{
				tIdx:           tIdx,
				validationCode: peer.TxValidationCode_INVALID_OTHER_REASON,
			}
			return
		}

		if chdr != nil {
			channel = chdr.ChannelId
			txType = common.HeaderType(chdr.Type)
		} else {
			channel = env.LeanEnv.ChannelId
			txType = env.Type
		}
		logger.Debugf("Transaction is for channel %s", channel)

		if !v.chainExists(channel) {
			logger.Errorf("Dropping transaction for non-existent channel %s", channel)
			results <- &blockValidationResult{
				tIdx:           tIdx,
				validationCode: peer.TxValidationCode_TARGET_CHAIN_NOT_FOUND,
			}
			return
		}

		switch txType {
		case common.HeaderType_ENDORSER_TRANSACTION:
			txID = env.LeanEnv.TxId

			// Check duplicate transactions
			// erroneousResultEntry := v.checkTxIdDupsLedger(tIdx, chdr, v.LedgerResources)
			// if erroneousResultEntry != nil {
			// 	results <- erroneousResultEntry
			// 	return
			// }

			// Validate tx with plugins
			logger.Debug("Validating transaction with plugins")

			// Dispatch happens here!!! change kro
			cde, err := v.DispatcherAdapter.DispatchLtx(tIdx, env, blockNum)
			if err != nil {
				logger.Errorf("Dispatch for transaction txId = %s returned error: %s", txID, err)
				switch err.(type) {
				case *commonerrors.VSCCExecutionFailureError:
					results <- &blockValidationResult{
						tIdx: tIdx,
						err:  err,
					}
					return
				case *commonerrors.VSCCInfoLookupFailureError:
					results <- &blockValidationResult{
						tIdx: tIdx,
						err:  err,
					}
					return
				default:
					results <- &blockValidationResult{
						tIdx:           tIdx,
						validationCode: cde,
					}
					return
				}
			}
		case common.HeaderType_CONFIG:
			commonPayload := payload.(*common.Payload)
			configEnvelope, err := configtx.UnmarshalConfigEnvelope(commonPayload.Data)
			if err != nil {
				err = errors.WithMessage(err, "error unmarshalling config which passed initial validity checks")
				logger.Criticalf("%+v", err)
				results <- &blockValidationResult{
					tIdx: tIdx,
					err:  err,
				}
				return
			}

			logger.Debugw("Config transaction envelope passed validation checks", "channel", channel)
			if err := v.ChannelResources.Apply(configEnvelope); err != nil {
				err = errors.WithMessage(err, "error validating config which passed initial validity checks")
				logger.Criticalf("%+v", err)
				results <- &blockValidationResult{
					tIdx: tIdx,
					err:  err,
				}
				return
			}
			logger.Infow("Config transaction validated and applied to channel resources", "channel", channel)
		default:
			logger.Warningf("Unknown transaction type [%s] in block number [%d] transaction index [%d]",
				common.HeaderType(chdr.Type), blockNum, tIdx)
			results <- &blockValidationResult{
				tIdx:           tIdx,
				validationCode: peer.TxValidationCode_UNKNOWN_TX_TYPE,
			}
			return
		}

		if _, err := proto.Marshal(env); err != nil {
			logger.Warningf("Cannot marshal transaction: %s", err)
			results <- &blockValidationResult{
				tIdx:           tIdx,
				validationCode: peer.TxValidationCode_MARSHAL_TX_ERROR,
			}
			return
		}
		// Succeeded to pass down here, transaction is valid
		results <- &blockValidationResult{
			tIdx:           tIdx,
			validationCode: peer.TxValidationCode_VALID,
			txid:           txID,
		}
		return
	} else {
		logger.Warning("Nil tx from block")
		results <- &blockValidationResult{
			tIdx:           tIdx,
			validationCode: peer.TxValidationCode_NIL_ENVELOPE,
		}
		return
	}
}

func (v *TxValidatorVSCCAdapter) Validate(vsccRequest *common.VsccRequest) *common.VsccResponse {

	logger.Debugf("[%s] START Block Validation for block [%d]", v.ChannelID, vsccRequest.BlockNum)

	results := make(chan *blockValidationResult, len(vsccRequest.Transactions))

	// leanEnabled, _ := strconv.ParseBool(os.Getenv("LEAN_ENABLED"))
	go func() {
		for index, txn := range vsccRequest.Transactions {
			// ensure that we don't have too many concurrent validation workers
			v.Semaphore.Acquire(context.Background())
			go func(index int, tIdx int, env *common.Envelope) {
				defer v.Semaphore.Release()
				// check for lean txn enabled or not
				if v.LeanEnabled {
					v.validateLtxTxn(&blockValidationRequestAdapter{
						blockNum: uint64(vsccRequest.BlockNum),
						tIdx:     tIdx,
						env:      env,
					}, results)
				} else {
					v.validateTx(&blockValidationRequestAdapter{
						blockNum: uint64(vsccRequest.BlockNum),
						tIdx:     tIdx,
						env:      env,
					}, results)
				}
			}(index, int(txn.TxIdx), txn.Env)
		}
	}()

	logger.Debugf("expecting %d block validation responses", len(vsccRequest.Transactions))

	vsccResponse := &common.VsccResponse{
		Status:              int32(codes.OK),
		ValidationResponses: make([]*common.ValidationResponse, len(vsccRequest.Transactions)),
	}

	// now we read responses in the order in which they come back
	for i := 0; i < len(vsccRequest.Transactions); i++ {

		res, ok := <-results
		if !ok {
			logger.Errorf("got terminal error, validation results channel is closed")
			vsccResponse.Status = int32(codes.Internal)
			return vsccResponse
		}

		if res.err != nil {
			// if there is an error, we buffer its value, wait for
			// all workers to complete validation and then return
			// the error from the first tx in this block that returned an error
			logger.Errorf("got terminal error %s for idx %d", res.err, res.tIdx)
			vsccResponse.Status = int32(codes.Internal)

		} else {
			// if there was no error, we set the txsfltr and we set the
			// txsChaincodeNames and txsUpgradedChaincodes maps
			logger.Debugf("got result for idx %d, code %d", res.tIdx, res.validationCode)
		}

		vsccResponse.ValidationResponses[i] = &common.ValidationResponse{
			TxIdx:          int32(res.tIdx),
			ValidationCode: int32(res.validationCode),
		}
	}

	return vsccResponse
}
