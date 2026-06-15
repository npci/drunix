/*
Copyright National Payments Corporation of India. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package gateway

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	gp "github.com/hyperledger/fabric-protos-go/gateway"
	"github.com/npci/drunix/common/deliver"
	"github.com/npci/drunix/protoutil"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (gs *Server) EndorseLeanTxn(ctx context.Context, request *gp.EndorseRequest) (*gp.EndorseResponse, error) {

	if request == nil {
		// gs.metrics.GatewayEndorseFailures.Add(1)
		return nil, status.Error(codes.InvalidArgument, "an endorse request is required")
	}
	signedProposal := request.GetProposedTransaction()
	if len(signedProposal.GetProposalBytes()) == 0 {
		// gs.metrics.GatewayEndorseFailures.Add(1)
		return nil, status.Error(codes.InvalidArgument, "the proposed transaction must contain a signed proposal")
	}
	proposal, err := protoutil.UnmarshalProposal(signedProposal.GetProposalBytes())
	if err != nil {
		// gs.metrics.GatewayEndorseFailures.Add(1)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	header, err := protoutil.UnmarshalHeader(proposal.GetHeader())
	if err != nil {
		// gs.metrics.GatewayEndorseFailures.Add(1)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	channelHeader, err := protoutil.UnmarshalChannelHeader(header.GetChannelHeader())
	if err != nil {
		// gs.metrics.GatewayEndorseFailures.Add(1)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	payloadBytes := proposal.GetPayload()
	payload, err := protoutil.UnmarshalChaincodeProposalPayload(payloadBytes)
	if err != nil {
		// gs.metrics.GatewayEndorseFailures.Add(1)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	spec, err := protoutil.UnmarshalChaincodeInvocationSpec(payload.GetInput())
	if err != nil {
		// gs.metrics.GatewayEndorseFailures.Add(1)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// channel := channelHeader.GetChannelId()
	// chaincodeID := spec.GetChaincodeSpec().GetChaincodeId()
	// hasTransientData := len(payload.GetTransientMap()) > 0

	channel := channelHeader.GetChannelId()
	chaincodeID := spec.GetChaincodeSpec().GetChaincodeId().GetName()
	hasTransientData := len(payload.GetTransientMap()) > 0

	logger := gs.logger.With("channel", channel, "chaincode", chaincodeID, "txID", request.GetTransactionId())

	var plan *plan
	var action *common.ChaincodeEndorsedAction
	if len(request.GetEndorsingOrganizations()) > 0 {
		// The client is specifying the endorsing orgs and taking responsibility for ensuring it meets the signature policy
		plan, err = gs.registry.planForOrgs(channel, chaincodeID, request.GetEndorsingOrganizations())
		if err != nil {
			// gs.metrics.GatewayEndorseFailures.Add(1)
			return nil, status.Error(codes.Unavailable, err.Error())
		}
	} else {
		// The client is delegating choice of endorsers to the gateway.
		plan, err = gs.planFromFirstEndorser(ctx, channel, chaincodeID, hasTransientData, signedProposal, logger)
		if err != nil {
			// gs.metrics.GatewayEndorseFailures.Add(1)
			return nil, err
		}
	}

	var endorsers []*endorser
	for plan.completedLayout == nil {
		// loop through the layouts until one gets satisfied
		endorsers = plan.endorsers()
		if endorsers == nil {
			// no more layouts
			break
		}
		// send to all the endorsers
		waitCh := make(chan bool, len(endorsers))
		for _, e := range endorsers {
			go func(e *endorser) {
				for e != nil {
					if gs.processProposal(ctx, plan, e, signedProposal, logger) {
						break
					}
					e = plan.nextPeerInGroup(e)
				}
				waitCh <- true
			}(e)
		}
		for i := 0; i < len(endorsers); i++ {
			select {
			case <-waitCh:
				// Endorser completedLayout normally
			case <-ctx.Done():
				// gs.metrics.GatewayEndorseFailures.Add(1)
				logger.Warnw("Endorse call timed out while collecting endorsements", "numEndorsers", len(endorsers))
				return nil, newRpcError(codes.DeadlineExceeded, "endorsement timeout expired while collecting endorsements")
			}
		}
	}
	if plan.completedLayout == nil {
		return nil, newRpcError(codes.Aborted, "failed to collect enough transaction endorsements, see attached details for more info", plan.errorDetails...)
	}

	mprp, err := protoutil.UnmarshalCommonProposalResponsePayload(plan.responsePayload)
	if err != nil {
		// gs.metrics.GatewayEndorseFailures.Add(1)
		return nil, errors.Wrap(err, "failed to unmarshal proposal response payload")
	}
	uniqEnd := uniqueEndorsements(plan.completedLayout.endorsements)
	uniqCommonEnd, err := protoutil.GetCommonEndorsements(uniqEnd)
	if err != nil {
		// gs.metrics.GatewayEndorseFailures.Add(1)
		return nil, newRpcError(codes.Aborted, fmt.Sprintf("failed to get common endorsements : %+v", err))
	}
	action = &common.ChaincodeEndorsedAction{ProposalResponsePayload: mprp, Endorsements: uniqCommonEnd}

	lHeader, err := protoutil.GetLiteHeader(header)
	if err != nil {
		// gs.metrics.GatewayEndorseFailures.Add(1)
		return nil, errors.Wrap(err, "failed to get light header")
	}
	ordererSigner := deliver.OrdererUserSigner()

	lenv := &common.LEnvelope{
		Header: lHeader,
		Data:   action,
	}
	/*
		DRUNIX:
			- format the lite envelope into a lean txn
			- events are being sent in the block instead of storing metadata in kvstore
	*/
	prepearedLeanTxn, err := prepareLeanTransaction(lenv, lHeader, ordererSigner)
	if err != nil {
		// gs.metrics.GatewayEndorseFailures.Add(1)
		return nil, errors.Wrap(err, "failed to prepare lean txn")
	}

	// leanEnvMeta := peer.LeanEnvMetadata{
	// 	EncodedKey:   encodedKey,
	// 	EncodedValue: encodedVal,
	// }

	// //store metadata in each endorsing org db
	// logger.Info("len of endorsers to insert metadata into : ", len(endorsers))
	// endorseErrorCh := make(chan error, len(endorsers))

	// for _, endor := range endorsers {
	// 	go func(e *endorser) {

	// 		logger.Debug("endorser msp : ", e.mspid)
	// 		ctx, cancel := context.WithTimeout(ctx, gs.options.EndorsementTimeout) // timeout of individual endorsement
	// 		defer cancel()
	// 		resp, err := e.client.InsertLeanEnvMetadata(ctx, &leanEnvMeta)
	// 		if err != nil {
	// 			gs.metrics.GatewayEndorseFailures.Add(1)
	// 			logger.Errorf("err inserting payload metadata into org db : %+v", err)
	// 		} else {
	// 			logger.Infof("inserted payload metadata into %s org db successfully? : %+v", e.mspid, resp.Response)
	// 		}
	// 		endorseErrorCh <- err
	// 	}(endor)

	// }

	// for i := 0; i < len(endorsers); i++ {
	// 	select {
	// 	case err := <-endorseErrorCh:
	// 		if err != nil {
	// 			gs.metrics.GatewayEndorseFailures.Add(1)
	// 			return nil, err
	// 		}
	// 		// metadata broadcast to endorsers completed normally
	// 	case <-ctx.Done():
	// 		gs.metrics.GatewayEndorseFailures.Add(1)
	// 		logger.Warnw("Endorse call timed out while broadcasting payload metadata to endorsers", "numEndorsers", len(endorsers))
	// 		return nil, newRpcError(codes.DeadlineExceeded, "endorsement timeout expired while broadcasting payload metadata to endorsers")
	// 	}
	// }

	/*
		calcTps.increment()
		calcTps.messageReceived <- true
	*/

	return &gp.EndorseResponse{PreparedTransaction: prepearedLeanTxn}, nil
}

func prepareLeanTransaction(lenv *common.LEnvelope, header *common.LightHeader, signer *deliver.SigningIdentity) (*common.Envelope, error) {

	leanEnvelope := &common.LeanEnvelope{
		ChannelId:    lenv.Header.ChannelHeader.ChannelId,
		TxId:         lenv.Header.ChannelHeader.TxId,
		ShardId:      lenv.Header.ChannelHeader.ShardId,
		OrgsInvolved: lenv.Header.ChannelHeader.OrgsInvolved,
		Nonce:        lenv.Header.SignatureHeader.Nonce,
		TlsCertHash:  lenv.Header.ChannelHeader.TlsCertHash,
		Results:      lenv.Data.ProposalResponsePayload.Extension.Results,
	}

	endorseMetatdata := &common.EnvMeta{
		SignatureHeader: lenv.Header.SignatureHeader,
		ChaincodeID:     lenv.Data.ProposalResponsePayload.Extension.ChaincodeId,
		Endorsements:    lenv.Data.Endorsements,
		ProposalHash:    lenv.Data.ProposalResponsePayload.ProposalHash,
		Events:          lenv.Data.ProposalResponsePayload.Extension.Events,
	}

	leanEnvelope.Meta = endorseMetatdata

	newPayload, err := proto.Marshal(leanEnvelope)
	if err != nil {
		logger.Error("err marshalling light env : ", err)
		return nil, err
	}

	// sign the payload
	sigN, err := signer.Sign(newPayload)
	if err != nil {
		return nil, err
	}

	// byteMeta, _ := json.Marshal(leanEnvelope.Meta)
	// logger.Debugf("size of meta of leanEnvelope: %d bytes (%.6f KB)\n", len(byteMeta), float64(len(byteMeta))/1024)

	// here's the envelope
	return &common.Envelope{
		Type:    common.HeaderType_ENDORSER_TRANSACTION,
		LeanEnv: leanEnvelope,
		//		Payload:   newPayload,
		Signature: sigN,
	}, nil
}

var (
	dataKeyPrefix = []byte{'d'}
	nsKeySep      = []byte{0x00}
)

/*
DRUNIX:

	custom methods for inserting metadata in db
	NOT BEING USED
*/
// func encodeDataKey(ns, key string) []byte {
// 	k := append(dataKeyPrefix, []byte(ns)...)
// 	k = append(k, nsKeySep...)
// 	return append(k, []byte(key)...)
// }

// func encodeValue(v *statedb.VersionedValue) ([]byte, error) {
// 	return proto.Marshal(
// 		&stateleveldb.DBValue{
// 			Value: v.Value,
// 		},
// 	)
// }
