/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
Modifications Copyright National Payments Corporation of India
*/
package builtin

import (
	"fmt"
	"strings"

	"github.com/hyperledger/fabric-protos-go/peer"
	txnVal "github.com/npci/drunix/core/committer/txvalidator/v20"
	endorsement "github.com/npci/drunix/core/handlers/endorsement/api"
	identities "github.com/npci/drunix/core/handlers/endorsement/api/identities"
	"github.com/npci/drunix/protoutil"
	"github.com/pkg/errors"
)

// DefaultEndorsementFactory returns an endorsement plugin factory which returns plugins
// that behave as the default endorsement system chaincode
type DefaultEndorsementFactory struct{}

// New returns an endorsement plugin that behaves as the default endorsement system chaincode
func (*DefaultEndorsementFactory) New() endorsement.Plugin {
	return &DefaultEndorsement{}
}

// DefaultEndorsement is an endorsement plugin that behaves as the default endorsement system chaincode
type DefaultEndorsement struct {
	identities.SigningIdentityFetcher
}

// Endorse signs the given payload(ProposalResponsePayload bytes), and optionally mutates it.
// Returns:
// The Endorsement: A signature over the payload, and an identity that is used to verify the signature
// The payload that was given as input (could be modified within this function)
// Or error on failure
func (e *DefaultEndorsement) Endorse(prpBytes []byte, sp *peer.SignedProposal) (*peer.Endorsement, []byte, error) {
	if txnVal.IsLtfEnabled.Load() {
		return e.EndorseLtx(prpBytes, sp)
	}
	signer, err := e.SigningIdentityForRequest(sp)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed fetching signing identity")
	}
	// serialize the signing identity
	identityBytes, err := signer.Serialize()
	if err != nil {
		return nil, nil, errors.Wrapf(err, "could not serialize the signing identity")
	}

	// sign the concatenation of the proposal response and the serialized endorser identity with this endorser's key
	signature, err := signer.Sign(append(prpBytes, identityBytes...))
	if err != nil {
		return nil, nil, errors.Wrapf(err, "could not sign the proposal response payload")
	}
	endorsement := &peer.Endorsement{Signature: signature, Endorser: identityBytes}
	return endorsement, prpBytes, nil
}

func (e *DefaultEndorsement) EndorseLtx(prpBytes []byte, sp *peer.SignedProposal) (*peer.Endorsement, []byte, error) {
	signer, err := e.SigningIdentityForRequest(sp)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed fetching signing identity")
	}

	/*
		DRUNIX:
			- convert proposal payload response to light prp
			- this is only for endorser txn, for config txn just sign the original proposal response payload
			- in endorsement sign the light prp and return the endorsement

	*/

	prp, err := protoutil.UnmarshalProposalResponsePayload(prpBytes)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to unmarshal prp")
	}
	litePrp, err := protoutil.GetProposalResponsePayloadForLightTxn(prp)
	if err == nil {
		marshaledlitePrp, err := protoutil.Marshal(litePrp)
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to marshal light prp")
		}
		endorsement, err := protoutil.GetPeerEndorsement(litePrp, signer)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get peer endorsement : %+v", err)
		}

		return endorsement, marshaledlitePrp, nil
	} else if strings.Contains(err.Error(), "config transaction") {
		// serialize the signing identity
		identityBytes, err := signer.Serialize()
		if err != nil {
			return nil, nil, errors.Wrapf(err, "could not serialize the signing identity")
		}

		// sign the concatenation of the proposal response and the serialized endorser identity with this endorser's key
		signature, err := signer.Sign(append(prpBytes, identityBytes...))
		if err != nil {
			return nil, nil, errors.Wrapf(err, "could not sign the proposal response payload")
		}
		endorsement := &peer.Endorsement{Signature: signature, Endorser: identityBytes}

		return endorsement, prpBytes, nil
	}
	return nil, nil, errors.Wrap(err, "failed to unmarshal prp for light txn")
}

// Init injects dependencies into the instance of the Plugin
func (e *DefaultEndorsement) Init(dependencies ...endorsement.Dependency) error {
	for _, dep := range dependencies {
		sIDFetcher, isSigningIdentityFetcher := dep.(identities.SigningIdentityFetcher)
		if !isSigningIdentityFetcher {
			continue
		}
		e.SigningIdentityFetcher = sIDFetcher
		return nil
	}
	return errors.New("could not find SigningIdentityFetcher in dependencies")
}
