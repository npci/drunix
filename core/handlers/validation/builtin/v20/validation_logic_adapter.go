/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/
package v20

import (
	"fmt"

	"github.com/hyperledger/fabric-protos-go/common"
	commonerrors "github.com/npci/drunix/common/errors"
	"github.com/npci/drunix/core/common/validation/statebased"
	vc "github.com/npci/drunix/core/handlers/validation/api/capabilities"
	vi "github.com/npci/drunix/core/handlers/validation/api/identities"
	vp "github.com/npci/drunix/core/handlers/validation/api/policies"
	vs "github.com/npci/drunix/core/handlers/validation/api/state"
	"github.com/npci/drunix/protoutil"
)

// Typically this will only be invoked once per peer
func NewAdapter(c vc.Capabilities, s vs.StateFetcher, d vi.IdentityDeserializer, pe vp.PolicyEvaluator, cor statebased.CollectionResources) *ValidatorAdapter {
	vpmgr := &statebased.KeyLevelValidationParameterManagerImpl{
		StateFetcher:     s,
		PolicyTranslator: &toApplicationPolicyTranslator{},
	}
	eval := statebased.NewV20EvaluatorAdapter(vpmgr, pe, cor, s)
	sbv := statebased.NewKeyLevelValidator(eval, vpmgr)

	return &ValidatorAdapter{
		Validator: &Validator{
			capabilities:        c,
			stateFetcher:        s,
			deserializer:        d,
			policyEvaluator:     pe,
			stateBasedValidator: sbv,
		},
	}
}

// Validator implements the default transaction validation policy,
// which is to check the correctness of the read-write set and the endorsement
// signatures against an endorsement policy that is supplied as argument to
// every invoke
type ValidatorAdapter struct {
	*Validator
}

// Validate validates the given envelope corresponding to a transaction with an endorsement
// policy as given in its serialized form.
// Note that in the case of dependencies in a block, such as tx_n modifying the endorsement policy
// for key a and tx_n+1 modifying the value of key a, Validate(tx_n+1) will block until Validate(tx_n)
// has been resolved. If working with a limited number of goroutines for parallel validation, ensure
// that they are allocated to transactions in ascending order.
func (vscc *ValidatorAdapter) Validate(
	blockNum uint64,
	envelope *common.Envelope,
	namespace string,
	txPosition int,
	actionPosition int,
	policyBytes []byte,
) commonerrors.TxValidationError {
	// vscc.stateBasedValidator.PreValidate(uint64(txPosition), block)

	va, err := vscc.extractValidationArtifacts(envelope, actionPosition)
	if err != nil {
		vscc.stateBasedValidator.PostValidate(namespace, blockNum, uint64(txPosition), err)
		return policyErr(err)
	}

	txverr := vscc.stateBasedValidator.Validate(
		namespace,
		blockNum,
		uint64(txPosition),
		va.rwset,
		va.prp,
		policyBytes,
		va.endorsements,
	)
	if txverr != nil {
		logger.Errorf("VSCC error: stateBasedValidatorAdapter.Validate failed, err %s", txverr)
		vscc.stateBasedValidator.PostValidate(namespace, blockNum, uint64(txPosition), txverr)
		return txverr
	}

	vscc.stateBasedValidator.PostValidate(namespace, blockNum, uint64(txPosition), nil)

	return nil
}

func (vscc *ValidatorAdapter) ValidateLtx(
	blockNum uint64,
	env *common.Envelope,
	namespace string,
	txPosition int,
	actionPosition int,
	policyBytes []byte,
) commonerrors.TxValidationError {
	// vscc.stateBasedValidator.PreValidate(uint64(txPosition), block)

	va, err := vscc.extractValidationArtifactsLtx(env, actionPosition)
	if err != nil {
		// vscc.stateBasedValidator.PostValidate(namespace, block.Header.Number, uint64(txPosition), err)
		return policyErr(err)
	}

	txverr := vscc.stateBasedValidator.ValidateLtx(
		namespace,
		blockNum,
		uint64(txPosition),
		va.rwset,
		va.prp,
		policyBytes,
		va.endorsements,
	)
	if txverr != nil {
		logger.Errorf("VSCC error: stateBasedValidator.Validate failed, err %s", txverr)
		// panic("VSCC Error")
		// vscc.stateBasedValidator.PostValidate(namespace, block.Header.Number, uint64(txPosition), txverr)
		return txverr
	}

	// vscc.stateBasedValidator.PostValidate(namespace, block.Header.Number, uint64(txPosition), nil)
	return nil
}

func (vscc *ValidatorAdapter) extractValidationArtifacts(
	env *common.Envelope,
	actionPosition int,
) (*validationArtifacts, error) {

	// ...and the payload...
	payl, err := protoutil.UnmarshalPayload(env.Payload)
	if err != nil {
		logger.Errorf("VSCC error: GetPayload failed, err %s", err)
		return nil, err
	}

	chdr, err := protoutil.UnmarshalChannelHeader(payl.Header.ChannelHeader)
	if err != nil {
		return nil, err
	}

	// validate the payload type
	if common.HeaderType(chdr.Type) != common.HeaderType_ENDORSER_TRANSACTION {
		logger.Errorf("Only Endorser Transactions are supported, provided type %d", chdr.Type)
		err = fmt.Errorf("Only Endorser Transactions are supported, provided type %d", chdr.Type)
		return nil, err
	}

	// ...and the transaction...
	tx, err := protoutil.UnmarshalTransaction(payl.Data)
	if err != nil {
		logger.Errorf("VSCC error: GetTransaction failed, err %s", err)
		return nil, err
	}

	cap, err := protoutil.UnmarshalChaincodeActionPayload(tx.Actions[actionPosition].Payload)
	if err != nil {
		logger.Errorf("VSCC error: GetChaincodeActionPayload failed, err %s", err)
		return nil, err
	}

	pRespPayload, err := protoutil.UnmarshalProposalResponsePayload(cap.Action.ProposalResponsePayload)
	if err != nil {
		err = fmt.Errorf("GetProposalResponsePayload error %s", err)
		return nil, err
	}
	if pRespPayload.Extension == nil {
		err = fmt.Errorf("nil pRespPayload.Extension")
		return nil, err
	}
	respPayload, err := protoutil.UnmarshalChaincodeAction(pRespPayload.Extension)
	if err != nil {
		err = fmt.Errorf("GetChaincodeAction error %s", err)
		return nil, err
	}

	return &validationArtifacts{
		rwset:        respPayload.Results,
		prp:          cap.Action.ProposalResponsePayload,
		endorsements: cap.Action.Endorsements,
		chdr:         chdr,
		env:          env,
		payl:         payl,
		cap:          cap,
	}, nil
}

func (vscc *ValidatorAdapter) extractValidationArtifactsLtx(
	env *common.Envelope,
	actionPosition int,
) (*validationArtifactsAdapter, error) {
	var ccAction *common.ChaincodeEndorsedAction
	var chdr *common.ChannelHeader
	var payl *common.Payload
	var proposalResponsePayload *common.ProposalResponsePayload
	// get the envelope...
	// env, err := protoutil.GetEnvelopeFromBlock(block.Data.Data[txPosition])
	// if err != nil {
	// 	logger.Errorf("VSCC error: GetEnvelope failed, err %s", err)
	// 	return nil, err
	// }

	// var lmetaData *common.EnvMeta

	// if env.LeanEnv.Meta == nil || env.LeanEnv.Meta.ProposalHash == nil {
	// 	envMeta.EnvMetaLock.RLock()
	// 	lmeta, exist := envMeta.EnvMetaMap[int(block.Header.Number)][env.LeanEnv.TxId+sep+string(env.LeanEnv.Nonce)]
	// 	envMeta.EnvMetaLock.RUnlock()

	// 	if exist {
	// 		lmetaData = lmeta
	// 	} else {

	// 		stateFetcher, err := vscc.stateFetcher.FetchState()
	// 		if err != nil {
	// 			logger.Error("Error in getting state fetcher")
	// 			return nil, err
	// 		}

	// 		defer stateFetcher.Done()
	// 		res, err := stateFetcher.GetStateMultipleKeys("", []string{env.LeanEnv.TxId + sep + string(env.LeanEnv.Nonce)})

	// 		if err != nil {
	// 			logger.Errorf("VSCC error: metadata fetch failed for tx:%s, err %s", env.LeanEnv.TxId, err)
	// 			return nil, err
	// 		}

	// 		if len(res) == 0 {
	// 			logger.Errorf("VSCC error: no metadata found for tx:%s", env.LeanEnv.TxId)
	// 			return nil, errors.Errorf("VSCC error: no metadata found for tx:%s", env.LeanEnv.TxId)
	// 		}

	// 		// env.LeanEnv.Meta, _ = protoutil.GetLeanMetadata(res[0])
	// 		lmetaData, _ = protoutil.GetLeanMetadata(res[0])

	// 		envMeta.EnvMetaLock.Lock()
	// 		envKey := env.LeanEnv.TxId + sep + string(env.LeanEnv.Nonce)
	// 		if _, ok := envMeta.EnvMetaMap[int(block.Header.Number)]; ok {
	// 			envMeta.EnvMetaMap[int(block.Header.Number)][envKey] = lmeta
	// 		} else {
	// 			envMeta.EnvMetaMap[int(block.Header.Number)] = map[string]*common.EnvMeta{
	// 				envKey: lmeta,
	// 			}
	// 		}
	// 		// envMeta.EnvMetaMap[int(block.Header.Number)][envKey] = env.LeanEnv.Meta
	// 		envMeta.EnvMetaLock.Unlock()

	// 	}
	// }

	// if lmetaData != nil {
	// 	env.LeanEnv.Meta.ProposalHash = lmetaData.ProposalHash
	// 	env.LeanEnv.Meta.Events = lmetaData.Events
	// 	// To keep the proposal bytes same as generated by lite peer
	// 	if env.LeanEnv.Meta.Events == nil {
	// 		env.LeanEnv.Meta.Events = &common.ChaincodeEvent{}
	// 	}
	// 	// env.LeanEnv.Meta.SignatureHeader = lmetaData.SignatureHeader
	// }

	var err error

	if env.LeanEnv.Meta.Events == nil {
		env.LeanEnv.Meta.Events = &common.ChaincodeEvent{}
	}

	if env.Type == common.HeaderType_ENDORSER_TRANSACTION {

		// to-decide (response is ignored)
		proposalResponsePayload = &common.ProposalResponsePayload{
			ProposalHash: env.LeanEnv.Meta.ProposalHash,
			Extension: &common.ChaincodeAction{
				Results: env.LeanEnv.Results,
				/*
					the events are included in the endorser signature digest and in the metadata.
					non-endorsing orgs won't have this metadata so the signature verification fails.
				*/
				// Events:      env.LeanEnv.Meta.Events,
				Events:      &common.ChaincodeEvent{},
				ChaincodeId: env.LeanEnv.Meta.ChaincodeID,
			},
		}
	} else {

		// ...and the payload...
		payl, err = protoutil.UnmarshalPayload(env.Payload)
		if err != nil {
			logger.Errorf("VSCC error: GetPayload failed, err %s", err)
			return nil, err
		}

		chdr, err = protoutil.UnmarshalChannelHeader(payl.Header.ChannelHeader)
		if err != nil {
			return nil, err
		}

		// validate the payload type
		if common.HeaderType(chdr.Type) != common.HeaderType_ENDORSER_TRANSACTION {
			logger.Errorf("Only Endorser Transactions are supported, provided type %d", chdr.Type)
			err = fmt.Errorf("Only Endorser Transactions are supported, provided type %d", chdr.Type)
			return nil, err
		}

		// ...and the transaction...
		tx, err := protoutil.UnmarshalTransaction(payl.Data)
		if err != nil {
			logger.Errorf("VSCC error: GetTransaction failed, err %s", err)
			return nil, err
		}

		cap, err := protoutil.UnmarshalChaincodeActionPayload(tx.Actions[actionPosition].Payload)
		if err != nil {
			logger.Errorf("VSCC error: GetChaincodeActionPayload failed, err %s", err)
			return nil, err
		}

		// pRespPayload, err := protoutil.UnmarshalProposalResponsePayload(cap.Action.ProposalResponsePayload)
		// if err != nil {
		// 	err = fmt.Errorf("GetProposalResponsePayload error %s", err)
		// 	return nil, err
		// }

		ccAction, err = protoutil.GetChaincodeEndorsedAction(cap.Action.Endorsements, cap.Action.ProposalResponsePayload)
		if err != nil {
			err = fmt.Errorf("light.GetChaincodeEndorsedAction failed")
			return nil, err
		}

		proposalResponsePayload = ccAction.ProposalResponsePayload
		// if pRespPayload.Extension == nil {
		// 	err = fmt.Errorf("nil pRespPayload.Extension")
		// 	return nil, err
		// }

		// respPayload, err := protoutil.UnmarshalChaincodeAction(pRespPayload.Extension)
		// if err != nil {
		// 	err = fmt.Errorf("GetChaincodeAction error %s", err)
		// 	return nil, err
		// }

		// rTxRWSet := &rwsetutil.TxRwSet{}
		// if err = rTxRWSet.FromProtoBytes(respPayload.Results); err != nil {
		// 	return nil, err
		// }

		// txRWSet, err := light.GettxRWSetForLightTxn(rTxRWSet)
		// if err != nil {
		// 	return nil, err
		// }

	}

	// to-decide (what to do with chdr & env)
	return &validationArtifactsAdapter{
		rwset:        env.LeanEnv.Results,
		prp:          proposalResponsePayload,
		endorsements: env.LeanEnv.Meta.Endorsements,
		chdr:         chdr,
		env:          env,
		payl:         payl,
		cap:          proposalResponsePayload.Extension,
	}, nil

}
