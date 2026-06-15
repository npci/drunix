/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/
package plugindispatcher

import (
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	commonerrors "github.com/npci/drunix/common/errors"
	validation "github.com/npci/drunix/core/handlers/validation/api"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/npci/drunix/protoutil"
	"github.com/pkg/errors"
)

type dispatcherImplAdapter struct {
	*dispatcherImpl
	pluginValidator *PluginValidatorAdapter
}

// New creates new plugin dispatcher
func NewAdapter(chainID string, cr ChannelResources, ler LedgerResources, lcr LifecycleResources, pluginValidator *PluginValidatorAdapter) *dispatcherImplAdapter {
	return &dispatcherImplAdapter{
		dispatcherImpl: &dispatcherImpl{
			chainID: chainID,
			cr:      cr,
			ler:     ler,
			lcr:     lcr,
		},
		pluginValidator: pluginValidator,
	}
}

func (v *dispatcherImplAdapter) DispatchLtx(seq int, env *common.Envelope, blockNum uint64) (peer.TxValidationCode, error) {
	// chainID := v.chainID
	// logger.Debugf("[%s] Dispatch starts for bytes %p", chainID, envBytes)
	// _, cHdr, _, err := light.GetChannelAndSignatureHeader(env)

	// get channel header
	//chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	// if err != nil {
	// 	return peer.TxValidationCode_BAD_CHANNEL_HEADER, err
	// }

	// // get header extensions so we have the chaincode ID
	// hdrExt, err := protoutil.UnmarshalChaincodeHeaderExtension(cHdr.Extension)
	// if err != nil {
	// 	return peer.TxValidationCode_BAD_HEADER_EXTENSION, err
	// }
	// Verify the header extension and response payload contain the ChaincodeId
	// if hdrExt.ChaincodeId == nil {
	// 	return peer.TxValidationCode_INVALID_OTHER_REASON, errors.New("nil ChaincodeId in header extension")
	// }

	var ccID, ccVer, ccIDHdr string
	var txRWSet *common.TxReadWriteSet
	var err error
	var channelId, txId string

	// fmt.Printf("131-------env.Type: %v\n", env.Type)
	if env.Type == common.HeaderType_ENDORSER_TRANSACTION {
		// respPayload := env.Lightenv.Data.ProposalResponsePayload.Extension
		// if respPayload.ChaincodeId == nil {
		// 	return peer.TxValidationCode_INVALID_OTHER_REASON, errors.New("nil ChaincodeId in ChaincodeAction")
		// }else{
		// 	ccVer = respPayload.ChaincodeId.Version
		// 	ccID = respPayload.ChaincodeId.Name
		// }

		// fmt.Println("141----leanEnv----", env.LeanEnv)
		ccEvent := &common.ChaincodeEvent{}
		if env.LeanEnv.Meta != nil {
			// fmt.Println("144----meta----", env.LeanEnv.Meta)
			// fmt.Println("145----meta.CC----", env.LeanEnv.Meta.ChaincodeID)
			ccVer = env.LeanEnv.Meta.ChaincodeID.Version
			ccID = env.LeanEnv.Meta.ChaincodeID.Name
			ccIDHdr = env.LeanEnv.Meta.ChaincodeID.Name
			ccEvent = env.LeanEnv.Meta.Events
		} else {
			respPayload, err := protoutil.GetActionFromEnvelopeMsg(env)
			if err != nil {
				return peer.TxValidationCode_BAD_RESPONSE_PAYLOAD, errors.WithMessage(err, "GetActionFromEnvelope failed")
			}
			if err = proto.Unmarshal(respPayload.Events, ccEvent); err != nil {
				return peer.TxValidationCode_INVALID_OTHER_REASON, errors.Wrapf(err, "invalid chaincode event")
			}
			ccID = respPayload.ChaincodeId.Name
			ccVer = respPayload.ChaincodeId.Version
			ccIDHdr = respPayload.ChaincodeId.Name
		}

		channelId = env.LeanEnv.ChannelId
		txId = env.LeanEnv.TxId
		txRWSet = env.LeanEnv.Results

		if ccEvent != nil && ccEvent.ChaincodeId != "" {
			if ccEvent.ChaincodeId != ccID {
				// TODO:
				return peer.TxValidationCode_INVALID_OTHER_REASON, errors.Errorf("chaincode event chaincode id: %s does not match chaincode action chaincode id: %s", ccID, "TBD")
			}
		}

	} else {
		// get name and version of the cc we invoked

		/* obtain the list of namespaces we're writing to */
		respPayload, err := protoutil.GetActionFromEnvelopeMsg(env)
		if err != nil {
			return peer.TxValidationCode_BAD_RESPONSE_PAYLOAD, errors.WithMessage(err, "GetActionFromEnvelope failed")
		}
		ccID = respPayload.ChaincodeId.Name
		ccVer = respPayload.ChaincodeId.Version
		ccIDHdr = respPayload.ChaincodeId.Name

		// if env.LeanEnv == nil {
		// 	lEnv, err := light.GetLightEnvFromCommonEnv(env)
		// 	if err != nil {
		// 		return peer.TxValidationCode_INVALID_OTHER_REASON, err
		// 	}
		// 	txId = lEnv.Header.ChannelHeader.ChannelId
		// 	channelId = lEnv.Header.ChannelHeader.TxId
		// }
		rTxRWSet := &rwsetutil.TxRwSet{}
		if err = rTxRWSet.FromProtoBytes(respPayload.Results); err != nil {
			return peer.TxValidationCode_BAD_RWSET, errors.WithMessage(err, "txRWSet.FromProtoBytes failed")
		}

		txRWSet = protoutil.GettxRWSetForLightTxn(rTxRWSet)
		ccEvent := &peer.ChaincodeEvent{}
		if err = proto.Unmarshal(respPayload.Events, ccEvent); err != nil {
			return peer.TxValidationCode_INVALID_OTHER_REASON, errors.Wrapf(err, "invalid chaincode event")
		}
		if ccEvent.ChaincodeId != "" && ccEvent.ChaincodeId != ccID {
			return peer.TxValidationCode_INVALID_OTHER_REASON, errors.Errorf("chaincode event chaincode id: %s does not match chaincode action chaincode id: %s", ccID, ccEvent.ChaincodeId)
		}
	}

	// sanity check on ccID
	if ccIDHdr == "" {
		err = errors.New("invalid chaincode ID")
		logger.Errorf("%+v", err)
		return peer.TxValidationCode_INVALID_CHAINCODE, err
	}
	if ccIDHdr != ccID {
		err = errors.Errorf("inconsistent ccid info (%s/%s)", ccID, ccIDHdr)
		logger.Errorf("%+v", err)
		return peer.TxValidationCode_INVALID_CHAINCODE, err
	}
	// sanity check on ccver
	if ccVer == "" {
		err = errors.New("invalid chaincode version")
		logger.Errorf("%+v", err)
		return peer.TxValidationCode_INVALID_CHAINCODE, err
	}

	wrNamespace := map[string]bool{}
	wrNamespace[ccID] = true

	namespaces := make(map[string]struct{})
	for _, ns := range txRWSet.NsRwset {
		// check to make sure there is no duplicate namespace in txRWSet
		if _, ok := namespaces[ns.Namespace]; ok {
			logger.Errorf("duplicate namespace '%s' in txRWSet", ns.Namespace)
			return peer.TxValidationCode_ILLEGAL_WRITESET,
				errors.Errorf("duplicate namespace '%s' in txRWSet", ns.Namespace)
		}
		namespaces[ns.Namespace] = struct{}{}

		if v.txWritesToNamespaceLtx(ns) {
			wrNamespace[ns.Namespace] = true
		}
	}

	// we've gathered all the info required to proceed to validation;
	// validation will behave differently depending on the chaincode

	// validate *EACH* read write set according to its chaincode's endorsement policy
	for ns := range wrNamespace {
		// Get latest chaincode validation plugin name and policy
		validationPlugin, args, err := v.GetInfoForValidateLtx(channelId, ns)
		if err != nil {
			logger.Errorf("GetInfoForValidate for txId = %s returned error: %+v", txId, err)
			return peer.TxValidationCode_INVALID_CHAINCODE, err
		}

		// invoke the plugin
		ctx := &ContextAdapter{
			Seq:        seq,
			Envelope:   env,
			BlockNum:   blockNum,
			TxID:       txId,
			Channel:    channelId,
			Namespace:  ns,
			Policy:     args,
			PluginName: validationPlugin,
		}
		if err = v.invokeValidationPluginLtx(ctx); err != nil {
			switch err.(type) {
			case *commonerrors.VSCCEndorsementPolicyError:
				return peer.TxValidationCode_ENDORSEMENT_POLICY_FAILURE, err
			default:
				return peer.TxValidationCode_INVALID_OTHER_REASON, err
			}
		}
	}

	// logger.Debugf("[%s] Dispatch completes env bytes %p", chainID, envBytes)
	return peer.TxValidationCode_VALID, nil
}

// Dispatch executes the validation plugin(s) for transaction
func (v *dispatcherImplAdapter) Dispatch(seq int, payload *common.Payload, env *common.Envelope, blockNum uint64) (peer.TxValidationCode, error) {
	chainID := v.chainID
	logger.Debugf("[%s] Dispatch starts for env %p", chainID, env)

	// get channel header
	chdr, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return peer.TxValidationCode_BAD_CHANNEL_HEADER, err
	}

	// get header extensions so we have the chaincode ID
	hdrExt, err := protoutil.UnmarshalChaincodeHeaderExtension(chdr.Extension)
	if err != nil {
		return peer.TxValidationCode_BAD_HEADER_EXTENSION, err
	}

	/* obtain the list of namespaces we're writing to */
	respPayload, err := protoutil.GetActionFromEnvelopeMsg(env)
	if err != nil {
		return peer.TxValidationCode_BAD_RESPONSE_PAYLOAD, errors.WithMessage(err, "GetActionFromEnvelope failed")
	}
	txRWSet := &rwsetutil.TxRwSet{}
	if err = txRWSet.FromProtoBytes(respPayload.Results); err != nil {
		return peer.TxValidationCode_BAD_RWSET, errors.WithMessage(err, "txRWSet.FromProtoBytes failed")
	}

	// Verify the header extension and response payload contain the ChaincodeId
	if hdrExt.ChaincodeId == nil {
		return peer.TxValidationCode_INVALID_OTHER_REASON, errors.New("nil ChaincodeId in header extension")
	}

	if respPayload.ChaincodeId == nil {
		return peer.TxValidationCode_INVALID_OTHER_REASON, errors.New("nil ChaincodeId in ChaincodeAction")
	}

	// get name and version of the cc we invoked
	ccID := hdrExt.ChaincodeId.Name
	ccVer := respPayload.ChaincodeId.Version

	// sanity check on ccID
	if ccID == "" {
		err = errors.New("invalid chaincode ID")
		logger.Errorf("%+v", err)
		return peer.TxValidationCode_INVALID_CHAINCODE, err
	}
	if ccID != respPayload.ChaincodeId.Name {
		err = errors.Errorf("inconsistent ccid info (%s/%s)", ccID, respPayload.ChaincodeId.Name)
		logger.Errorf("%+v", err)
		return peer.TxValidationCode_INVALID_CHAINCODE, err
	}
	// sanity check on ccver
	if ccVer == "" {
		err = errors.New("invalid chaincode version")
		logger.Errorf("%+v", err)
		return peer.TxValidationCode_INVALID_CHAINCODE, err
	}

	wrNamespace := map[string]bool{}
	wrNamespace[ccID] = true
	if respPayload.Events != nil {
		ccEvent := &peer.ChaincodeEvent{}
		if err = proto.Unmarshal(respPayload.Events, ccEvent); err != nil {
			return peer.TxValidationCode_INVALID_OTHER_REASON, errors.Wrapf(err, "invalid chaincode event")
		}
		if ccEvent.ChaincodeId != ccID {
			return peer.TxValidationCode_INVALID_OTHER_REASON, errors.Errorf("chaincode event chaincode id does not match chaincode action chaincode id")
		}
	}

	namespaces := make(map[string]struct{})
	for _, ns := range txRWSet.NsRwSets {
		// check to make sure there is no duplicate namespace in txRWSet
		if _, ok := namespaces[ns.NameSpace]; ok {
			logger.Errorf("duplicate namespace '%s' in txRWSet", ns.NameSpace)
			return peer.TxValidationCode_ILLEGAL_WRITESET,
				errors.Errorf("duplicate namespace '%s' in txRWSet", ns.NameSpace)
		}
		namespaces[ns.NameSpace] = struct{}{}

		if v.txWritesToNamespace(ns) {
			wrNamespace[ns.NameSpace] = true
		}
	}

	// we've gathered all the info required to proceed to validation;
	// validation will behave differently depending on the chaincode

	// validate *EACH* read write set according to its chaincode's endorsement policy
	for ns := range wrNamespace {
		// Get latest chaincode validation plugin name and policy
		validationPlugin, args, err := v.GetInfoForValidate(chdr, ns)
		if err != nil {
			logger.Errorf("GetInfoForValidate for txId = %s returned error: %+v", chdr.TxId, err)
			return peer.TxValidationCode_INVALID_CHAINCODE, err
		}

		// invoke the plugin
		ctx := &ContextAdapter{
			Seq:        seq,
			Envelope:   env,
			BlockNum:   blockNum,
			TxID:       chdr.TxId,
			Channel:    chdr.ChannelId,
			Namespace:  ns,
			Policy:     args,
			PluginName: validationPlugin,
		}

		if err = v.invokeValidationPlugin(ctx); err != nil {
			switch err.(type) {
			case *commonerrors.VSCCEndorsementPolicyError:
				return peer.TxValidationCode_ENDORSEMENT_POLICY_FAILURE, err
			default:
				return peer.TxValidationCode_INVALID_OTHER_REASON, err
			}
		}
	}

	logger.Debugf("[%s] Dispatch completes env %p", chainID, env)
	return peer.TxValidationCode_VALID, nil
}

func (v *dispatcherImplAdapter) invokeValidationPlugin(ctx *ContextAdapter) error {
	logger.Debug("Validating", ctx, "with plugin")
	err := v.pluginValidator.ValidateWithPlugin(ctx)
	if err == nil {
		return nil
	}
	// If the error is a pluggable validation execution error, cast it to the common errors ExecutionFailureError.
	if e, isExecutionError := err.(*validation.ExecutionFailureError); isExecutionError {
		return &commonerrors.VSCCExecutionFailureError{Err: e}
	}
	// Else, treat it as an endorsement error.
	return &commonerrors.VSCCEndorsementPolicyError{Err: err}
}

func (v *dispatcherImplAdapter) invokeValidationPluginLtx(ctx *ContextAdapter) error {
	/*
		DRUNIX
		logger Debug is having high cpu usage compared to the Debugf
		even service is not running in debug mode
	*/
	logger.Debugf("Validating %v with plugin", ctx)
	err := v.pluginValidator.ValidateWithPluginLtx(ctx)
	if err == nil {
		return nil
	}
	// If the error is a pluggable validation execution error, cast it to the common errors ExecutionFailureError.
	if e, isExecutionError := err.(*validation.ExecutionFailureError); isExecutionError {
		return &commonerrors.VSCCExecutionFailureError{Err: e}
	}
	// Else, treat it as an endorsement error.
	return &commonerrors.VSCCEndorsementPolicyError{Err: err}
}
