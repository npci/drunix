/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/
package txvalidator

import (
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	commonerrors "github.com/npci/drunix/common/errors"
	"github.com/npci/drunix/core/common/validation"
	"github.com/npci/drunix/protoutil"
)

func (v *TxValidator) validateLtxTx(req *blockValidationRequest, results chan<- *blockValidationResult) {
	block := req.block
	d := req.d
	tIdx := req.tIdx
	txID := ""

	if d == nil {
		results <- &blockValidationResult{
			tIdx: tIdx,
		}
		return
	}

	if env, err := protoutil.GetEnvelopeFromBlock(d); err != nil {
		logger.Warningf("Error getting tx from block: %+v", err)
		results <- &blockValidationResult{
			tIdx:           tIdx,
			validationCode: peer.TxValidationCode_INVALID_OTHER_REASON,
		}
		return
		//DRUNIX: for config, approve, commit, vanila txns, lean env is nil, so txn goes to vanila validation flow
	} else if env != nil && env.LeanEnv == nil {
		v.validateTx(req, results)
		return
	} else if env != nil {
		// validate the transaction: here we check that the transaction
		// is properly formed, properly signed and that the security
		// chain binding proposal to endorsements to tx holds. We do
		// NOT check the validity of endorsements, though. That's a
		// job for the validation plugins
		logger.Debugf("[%s] validateTx starts for block %p env %p txn %d", v.ChannelID, block, env, tIdx)
		defer logger.Debugf("[%s] validateTx completes for block %p env %p txn %d", v.ChannelID, block, env, tIdx)
		var txResult peer.TxValidationCode

		if txResult = validation.ValidateLtxTransaction(env, v.CryptoProvider); txResult != peer.TxValidationCode_VALID {
			logger.Errorf("Invalid transaction with index %d", tIdx)
			results <- &blockValidationResult{
				tIdx:           tIdx,
				validationCode: txResult,
			}
			return
		}

		channel := env.LeanEnv.ChannelId
		logger.Debugf("Transaction is for channel %s", channel)

		if !v.chainExists(channel) {
			logger.Errorf("Dropping transaction for non-existent channel %s", channel)
			results <- &blockValidationResult{
				tIdx:           tIdx,
				validationCode: peer.TxValidationCode_TARGET_CHAIN_NOT_FOUND,
			}
			return
		}

		if common.HeaderType(env.Type) == common.HeaderType_ENDORSER_TRANSACTION {

			txID = env.LeanEnv.TxId

			// Check duplicate transactions
			erroneousResultEntry := v.checkLtxIdDupsLedger(tIdx, txID, v.LedgerResources)
			if erroneousResultEntry != nil {
				results <- erroneousResultEntry
				return
			}

			// Validate tx with plugins
			logger.Debug("Validating transaction with plugins")
			cde, err := v.DispatcherAdapter.DispatchLtx(tIdx, env, block.Header.Number)
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
		} else {
			logger.Warningf("Unknown transaction type [%s] in block number [%d] transaction index [%d]",
				common.HeaderType(env.Type), block.Header.Number, tIdx)
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
