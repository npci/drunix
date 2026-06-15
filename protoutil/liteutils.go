/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/
package protoutil

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/pkg/errors"
)

// /*
// DRUNIX:
// 	- this pkg is specifically for lite modifications done in the peer
// 	- high level description of this pkg is that this pkg converts the data structures `Endorsement`, `KVRWSet`, `CollHashedRwSet`, `TxRwSet`, `ChaincodeAction`, `ProposalResponsePayload` from peer, kvrwset, rwsetutil packages into the simailar signature data structures in common pkg
// 	- convert the `common.Envelope` to `common.LeanEnvelope`

// 	Nothing fancy happening. Just unmarshal, de-reference the vanilla fabric data structures and then assign the data to the lite format data structures
// */

func GetCommonEndorsement(pEnd *peer.Endorsement) *common.Endorsement {
	cEnd := &common.Endorsement{}

	cEnd.Signature = pEnd.Signature

	cEnd.Endorser = &common.SerializedIdentity{}

	proto.Unmarshal(pEnd.Endorser, cEnd.Endorser)

	return cEnd
}

func GetEndorsementsForLightTxn(endorsements []*peer.Endorsement) []*common.Endorsement {
	cEnd := []*common.Endorsement{}
	for _, pEnd := range endorsements {
		cEnd = append(cEnd, GetCommonEndorsement(pEnd))
	}

	return cEnd
}

func GetCommonVersion(kVersion *kvrwset.Version) *common.Version {

	if kVersion == nil {
		return nil
	} else {
		return &common.Version{
			BlockNum: kVersion.BlockNum,
			TxNum:    kVersion.TxNum,
		}
	}
}

func GetHRWSet(hrwset *kvrwset.HashedRWSet) *common.HashedRWSet {

	cNs := &common.HashedRWSet{
		HashedReads:    []*common.KVReadHash{},
		HashedWrites:   []*common.KVWriteHash{},
		MetadataWrites: []*common.KVMetadataWriteHash{},
	}

	for _, rd := range hrwset.HashedReads {
		cVersion := GetCommonVersion(rd.Version)
		cNs.HashedReads = append(cNs.HashedReads, &common.KVReadHash{
			KeyHash: rd.KeyHash,
			Version: cVersion,
		})

	}
	for _, wr := range hrwset.HashedWrites {
		cNs.HashedWrites = append(cNs.HashedWrites, &common.KVWriteHash{
			KeyHash:   wr.KeyHash,
			IsDelete:  wr.IsDelete,
			ValueHash: wr.ValueHash,
		})
	}

	return cNs

}

func GetCollHRWSet(hrwset *rwsetutil.CollHashedRwSet) *common.CollectionHashedReadWriteSet {

	cNs := &common.CollectionHashedReadWriteSet{
		CollectionName: hrwset.CollectionName,
		HashedRwset:    GetHRWSet(hrwset.HashedRwSet),
		PvtRwsetHash:   hrwset.PvtRwSetHash,
	}

	return cNs

}

func GettxRWSetForLightTxn(txRWSet *rwsetutil.TxRwSet) *common.TxReadWriteSet {

	cTxRWSet := &common.TxReadWriteSet{
		NsRwset: []*common.NsReadWriteSet{},
	}

	for _, ns := range txRWSet.NsRwSets {
		cTxRWSet.NsRwset = append(cTxRWSet.NsRwset, GetNSForLT(ns))
	}

	return cTxRWSet
}

func GetChaincodeEndorsedAction(endorsements []*peer.Endorsement, prpBytes []byte) (*common.ChaincodeEndorsedAction, error) {

	pRespPayload, err := UnmarshalProposalResponsePayload(prpBytes)
	if err != nil {
		return nil, err
	}

	lPrp, err := GetProposalResponsePayloadForLightTxn(pRespPayload)
	if err != nil {
		return nil, err
	}

	lEndorsements := GetEndorsementsForLightTxn(endorsements)

	return &common.ChaincodeEndorsedAction{
		ProposalResponsePayload: lPrp,
		Endorsements:            lEndorsements,
		XXX_NoUnkeyedLiteral:    struct{}{},
		XXX_unrecognized:        []byte{},
		XXX_sizecache:           0,
	}, nil

}

func GetLightEnvFromCommonEnv(txnEnv *common.Envelope) (*common.LEnvelope, error) {

	var txnHdr *common.LightHeader
	var payload *common.Payload
	var err error

	if payload, err = UnmarshalPayload(txnEnv.Payload); err == nil {
		txnHdr, err = GetLiteHeader(payload.Header)
		if err != nil {
			fmt.Printf("error while unmarshalling:%v\n\n\n", err)
			return nil, err
		}

	}

	txn, err := UnmarshalLightTransaction(payload.Data)
	if err != nil {
		fmt.Printf("423error unmarshalling txn:%v\n\n\n", err)
		return nil, err
	}

	lightEnv := &common.LEnvelope{
		Header: txnHdr,
		Data:   txn,
	}

	return lightEnv, nil
}

func GetLeanEnvFromCommonEnv(txnEnv *common.Envelope) (*common.LeanEnvelope, error) {
	lightEnv, err := GetLightEnvFromCommonEnv(txnEnv)
	if err != nil {
		return nil, err
	}
	// Check if env is of Normal Or Commit-Approval
	meta := &common.EnvMeta{
		SignatureHeader: lightEnv.Header.SignatureHeader,
		ChaincodeID:     lightEnv.Data.ProposalResponsePayload.Extension.ChaincodeId,
		Endorsements:    lightEnv.Data.Endorsements,
		ProposalHash:    lightEnv.Data.ProposalResponsePayload.ProposalHash,
		Events:          lightEnv.Data.ProposalResponsePayload.Extension.Events,
	}

	leanEnv := &common.LeanEnvelope{
		ChannelId:    lightEnv.Header.ChannelHeader.ChannelId,
		TxId:         lightEnv.Header.ChannelHeader.TxId,
		ShardId:      lightEnv.Header.ChannelHeader.ShardId,
		OrgsInvolved: lightEnv.Header.ChannelHeader.OrgsInvolved,
		Nonce:        lightEnv.Header.SignatureHeader.Nonce,
		TlsCertHash:  lightEnv.Header.ChannelHeader.TlsCertHash,
		Results:      lightEnv.Data.ProposalResponsePayload.Extension.Results,
		Meta:         meta,
	}
	return leanEnv, nil
}

func UnmarshalLightTransaction(txBytes []byte) (*common.ChaincodeEndorsedAction, error) {
	tx := &peer.Transaction{}
	err := proto.Unmarshal(txBytes, tx)
	if err != nil {
		return nil, err
	}

	tx1 := &peer.ChaincodeActionPayload{}
	err = proto.Unmarshal(tx.Actions[0].Payload, tx1)
	if err != nil {
		return nil, err
	}

	return GetChaincodeEndorsedAction(tx1.Action.Endorsements, tx1.Action.ProposalResponsePayload)
}

/*
DRUNIX:

	Sign the lite format proposal response payload
*/
func GetPeerEndorsement(prp *common.ProposalResponsePayload, signer Signer) (*peer.Endorsement, error) {

	signerIDDetails, err := signer.Serialize()
	if err != nil {
		return nil, fmt.Errorf("Failed to get Signing Identity details from Signer : %+v", err)
	}
	cEnd := &peer.Endorsement{
		Endorser:  signerIDDetails,
		Signature: []byte{},
	}

	/*
		DRUNIX :
			including empty events in the endorser signature payload
			so that at the non-endorsing orgs the endorser signature validation happens propoerly.
	*/
	prp.Extension.Events = &common.ChaincodeEvent{}

	data, err := proto.Marshal(prp)
	if err != nil {
		return nil, fmt.Errorf("Failed to marshal proposalResponsePayload data for signing : %+v", err)
	}

	data = append(data, cEnd.Endorser...)
	signature, err := signer.Sign(data)
	if err != nil {
		return nil, fmt.Errorf("Failed to sign proposalResponsePayload : %+v", err)
	}

	cEnd.Signature = signature

	return cEnd, nil
}

/*
DRUNIX:

	Nothing fancy happening. Unmarshal, de-reference the vanilla RWSET, events and assign them to lite format chaincode action
*/
func GetChaincodeActionForLightTxn(ca *peer.ChaincodeAction) (*common.ChaincodeAction, error) {

	txRWSet := &rwsetutil.TxRwSet{}
	if err := txRWSet.FromProtoBytes(ca.Results); err != nil {
		return nil, fmt.Errorf("The transaction is rejected as the respPayload couldnt be unmarshalled : %v\n", ca)
	}

	cTxRWSet, err := gettxRWSetForLightTxn(txRWSet)
	if err != nil {
		return nil, err
	}
	pChaincodeEvent, _ := UnmarshalChaincodeEvents(ca.Events)
	commonCCAction := &common.ChaincodeAction{
		Results: cTxRWSet,
		Events: &common.ChaincodeEvent{
			ChaincodeId: pChaincodeEvent.ChaincodeId,
			TxId:        pChaincodeEvent.TxId,
			EventName:   pChaincodeEvent.EventName,
			Payload:     pChaincodeEvent.Payload,
		},
		// Response: &common.Response{
		// 	Status:  ca.Response.Status,
		// 	Message: ca.Response.Message,
		// 	Payload: ca.Response.Payload,
		// },
		ChaincodeId: &common.ChaincodeID{
			Path:    ca.ChaincodeId.Path,
			Name:    ca.ChaincodeId.Name,
			Version: ca.ChaincodeId.Version,
		},
	}

	return commonCCAction, nil

}

/*
DRUNIX:

	Convert vanilla RWSet to lite format RWSet
	This method internally calls
		- GetNSForLT
		- getCollHRWSet
		- GetKVRWSet
		- getHRWSet
	These methods does nothing fancy. It just de-references the vanilla RWSet and assign the data to the lite RWSet
*/
func gettxRWSetForLightTxn(txRWSet *rwsetutil.TxRwSet) (*common.TxReadWriteSet, error) {

	cTxRWSet := &common.TxReadWriteSet{
		NsRwset: []*common.NsReadWriteSet{},
	}

	//TODO: needs to be handled for CC upgrade
	// if txRWSet.NsRwSets[1].NameSpace == "lscc" {
	// 	fmt.Println("lscc came!!! 173--------")
	// 	return nil, errors.New("config transaction")
	// }

	for _, ns := range txRWSet.NsRwSets {
		cTxRWSet.NsRwset = append(cTxRWSet.NsRwset, GetNSForLT(ns))
	}

	return cTxRWSet, nil
}

func GetNSForLT(ns *rwsetutil.NsRwSet) *common.NsReadWriteSet {

	cNs := common.NsReadWriteSet{
		Namespace:             ns.NameSpace,
		Rwset:                 GetKVRWSet(ns.KvRwSet),
		CollectionHashedRwset: []*common.CollectionHashedReadWriteSet{},
	}

	for _, collhs := range ns.CollHashedRwSets {
		cNs.CollectionHashedRwset = append(cNs.CollectionHashedRwset, getCollHRWSet(collhs))
	}

	return &cNs

}

func getCollHRWSet(hrwset *rwsetutil.CollHashedRwSet) *common.CollectionHashedReadWriteSet {
	cNs := &common.CollectionHashedReadWriteSet{
		CollectionName: hrwset.CollectionName,
		HashedRwset:    getHRWSet(hrwset.HashedRwSet),
		PvtRwsetHash:   hrwset.PvtRwSetHash,
	}

	return cNs

}

func GetKVRWSet(rwset *kvrwset.KVRWSet) *common.KVRWSet {

	cNs := &common.KVRWSet{
		Reads:            []*common.KVRead{},
		RangeQueriesInfo: []*common.RangeQueryInfo{},
		Writes:           []*common.KVWrite{},
		MetadataWrites:   []*common.KVMetadataWrite{},
	}

	for _, rd := range rwset.Reads {
		var version *common.Version
		if rd.Version != nil {
			version = &common.Version{
				BlockNum: rd.Version.BlockNum,
				TxNum:    rd.Version.TxNum,
			}
		}
		cNs.Reads = append(cNs.Reads, &common.KVRead{
			Key: rd.Key,

			Version: version,
		})
	}
	for _, wr := range rwset.Writes {
		cNs.Writes = append(cNs.Writes, &common.KVWrite{
			Key:      wr.Key,
			IsDelete: wr.IsDelete,
			Value:    wr.Value,
		})
	}

	return cNs

}

func getHRWSet(hrwset *kvrwset.HashedRWSet) *common.HashedRWSet {

	cNs := &common.HashedRWSet{
		HashedReads:    []*common.KVReadHash{},
		HashedWrites:   []*common.KVWriteHash{},
		MetadataWrites: []*common.KVMetadataWriteHash{},
	}

	for _, rd := range hrwset.HashedReads {

		var version *common.Version
		if rd.Version != nil {
			version = &common.Version{
				BlockNum: rd.Version.BlockNum,
				TxNum:    rd.Version.TxNum,
			}

		}
		cNs.HashedReads = append(cNs.HashedReads, &common.KVReadHash{
			KeyHash: rd.KeyHash,
			Version: version,
		})

	}
	for _, wr := range hrwset.HashedWrites {
		cNs.HashedWrites = append(cNs.HashedWrites, &common.KVWriteHash{
			KeyHash:   wr.KeyHash,
			IsDelete:  wr.IsDelete,
			ValueHash: wr.ValueHash,
		})
	}

	return cNs

}

/*
DRUNIX:

	This and it's dependent methods does nothing fancy. They just unmarshal and de-reference the vanilla fabric data structures and then assign the de-referenced values to the lite data structures

	We return the lite proposal payload from endorser plugin. This lite format PRP is received by processProposal and then included in the lite chaincode endorsed action
*/
func GetProposalResponsePayloadForLightTxn(prp *peer.ProposalResponsePayload) (*common.ProposalResponsePayload, error) {
	peerCcodeAction, err := UnmarshalChaincodeAction(prp.Extension)
	if err != nil {
		return nil, fmt.Errorf("err unmarsal proposal response payload for light txn : %+v\n", err)
	}

	chaincodeAction, err := GetChaincodeActionForLightTxn(peerCcodeAction)
	if err != nil {
		return nil, err
	}

	lPrp := &common.ProposalResponsePayload{
		ProposalHash: prp.ProposalHash,
		Extension:    chaincodeAction,
	}

	return lPrp, nil
}

/*
DRUNIX:

	the ProcessProposal method after receiving the endorsements from the required endorsers will form a lean txn and return.
	This lean txn requires lite format chaincode endorsed action which inturn requires lite format PRP
	This methos unmarshall the PRP bytes to lite format PRP
*/
func UnmarshalCommonProposalResponsePayload(prpBytes []byte) (*common.ProposalResponsePayload, error) {
	prp := &common.ProposalResponsePayload{}
	err := proto.Unmarshal(prpBytes, prp)
	return prp, errors.Wrap(err, "error unmarshalling ProposalResponsePayload")
}

// DRUNIX: nothing fancy just unmarshal and de-reference vanilla endorsements and assign the data to the lite endorsements
func GetCommonEndorsements(endorsements []*peer.Endorsement) ([]*common.Endorsement, error) {
	commonEndorsements := []*common.Endorsement{}

	for _, endorsement := range endorsements {
		identity, err := UnmarshalCommonSerializedIdentity(endorsement.Endorser)
		if err != nil {
			return nil, err
		}
		end := &common.Endorsement{
			Endorser:  identity,
			Signature: endorsement.Signature,
		}
		commonEndorsements = append(commonEndorsements, end)
	}
	return commonEndorsements, nil
}

func UnmarshalCommonSerializedIdentity(bytes []byte) (*common.SerializedIdentity, error) {
	sid := &common.SerializedIdentity{}
	err := proto.Unmarshal(bytes, sid)
	return sid, errors.Wrap(err, "error unmarshalling SerializedIdentity")
}

// DRUNIX: nothing fancy. Unmarshall and assign the data to lite header
func GetLiteHeader(hdr *common.Header) (*common.LightHeader, error) {

	shdr, err := UnmarshalSignatureHeader(hdr.SignatureHeader)
	if err != nil {
		return nil, err
	}

	chdr, err := UnmarshalChannelHeader(hdr.ChannelHeader)
	if err != nil {
		return nil, err
	}

	return &common.LightHeader{
		ChannelHeader:   chdr,
		SignatureHeader: shdr,
	}, nil

}

func GetLightHeader(hdr *common.Header) (*common.LightHeader, error) {

	shdr, err := UnmarshalSignatureHeader(hdr.SignatureHeader)
	if err != nil {
		return nil, err
	}

	chdr, err := UnmarshalChannelHeader(hdr.ChannelHeader)
	if err != nil {
		return nil, err
	}

	return &common.LightHeader{
		ChannelHeader:   chdr,
		SignatureHeader: shdr,
	}, nil

}

func GetNSForLT1(ns *rwsetutil.NsRwSet) *common.NsReadWriteSet {

	cNs := common.NsReadWriteSet{
		Namespace:             ns.NameSpace,
		Rwset:                 GetKVRWSet(ns.KvRwSet),
		CollectionHashedRwset: []*common.CollectionHashedReadWriteSet{},
	}

	for hIdx, hs := range ns.CollHashedRwSets {
		fmt.Println(hIdx, "The collection starting ", hs.CollectionName)
		cNs.CollectionHashedRwset = append(cNs.CollectionHashedRwset, getCollHRWSet(hs))
	}

	return &cNs

}

func EndorsementAsSignedDataForLightEnv(endorsement *common.Endorsement, prpBytes []byte) ([]*SignedData, error) {

	var err error
	inEndBytes, err := proto.Marshal(endorsement.Endorser)
	if err != nil {
		return nil, err
	}

	return []*SignedData{{
		Data:      append(prpBytes, inEndBytes...),
		Identity:  inEndBytes,
		Signature: endorsement.Signature,
	}}, nil
}

func GetChannelAndSignatureHeader(env *common.Envelope) (proto.Message, *common.ChannelHeader, *common.SignatureHeader, error) {

	if env.Type == common.HeaderType_ENDORSER_TRANSACTION {
		return env.LeanEnv, nil, nil, nil
	} else {
		payload, err := UnmarshalPayload(env.Payload)
		if err != nil {
			return payload, nil, nil, err
		}

		if payload.Header == nil {
			return payload, nil, nil, errors.New("envelope must have a Header")
		}

		chdr, err := UnmarshalChannelHeader(payload.Header.ChannelHeader)
		if err != nil {
			return payload, chdr, nil, err
		}

		shdr, err := UnmarshalSignatureHeader(payload.Header.SignatureHeader)
		if err != nil {
			return payload, chdr, shdr, err
		}
		return payload, chdr, shdr, nil

	}
}
