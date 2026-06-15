/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
Modifications Copyright National Payments Corporation of India
*/
package statebased

import (
	"github.com/hyperledger/fabric-protos-go/common"
	commonerrors "github.com/npci/drunix/common/errors"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/npci/drunix/protoutil"
	"github.com/pkg/errors"
)

type baseEvaluatorAdapter struct {
	*baseEvaluator
}

func (p *baseEvaluatorAdapter) checkSBAndCCEP(cc, coll, key string, blockNum, txNum uint64, signatureSet []*protoutil.SignedData) commonerrors.TxValidationError {

	vp := make([]byte, 0)
	var err error

	// // see if there is a key-level validation parameter for this key
	// vp, err := p.vpmgr.GetValidationParameterForKey(cc, coll, key, blockNum, txNum)
	// if err != nil {
	// 	// error handling for GetValidationParameterForKey follows this rationale:
	// 	switch err := errors.Cause(err).(type) {
	// 	// 1) if there is a conflict because validation params have been updated
	// 	//    by another transaction in this block, we will get ValidationParameterUpdatedError.
	// 	//    This should lead to invalidating the transaction by calling policyErr
	// 	case *ValidationParameterUpdatedError:
	// 		return policyErr(err)
	// 	// 2) if the ledger returns "determinstic" errors, that is, errors that
	// 	//    every peer in the channel will also return (such as errors linked to
	// 	//    an attempt to retrieve metadata from a non-defined collection) should be
	// 	//    logged and ignored. The ledger will take the most appropriate action
	// 	//    when performing its side of the validation.
	// 	case *ledger.CollConfigNotDefinedError, *ledger.InvalidCollNameError:
	// 		logger.Warning(errors.WithMessage(err, "skipping key-level validation").Error())
	// 	// 3) any other type of error should return an execution failure which will
	// 	//    lead to halting the processing on this channel. Note that any non-categorized
	// 	//    deterministic error would be caught by the default and would lead to
	// 	//    a processing halt. This would certainly be a bug, but - in the absence of a
	// 	//    single, well-defined deterministic error returned by the ledger, it is
	// 	//    best to err on the side of caution and rather halt processing (because a
	// 	//    deterministic error is treated like an I/O one) rather than risking a fork
	// 	//    (in case an I/O error is treated as a deterministic one).
	// 	default:
	// 		return &commonerrors.VSCCExecutionFailureError{
	// 			Err: err,
	// 		}
	// 	}
	// }

	// if no key-level validation parameter has been specified, the regular cc endorsement policy needs to hold
	if len(vp) == 0 {
		return p.CheckCCEPIfNotChecked(cc, coll, blockNum, txNum, signatureSet)
	}

	// validate against key-level vp
	err = p.policySupport.Evaluate(vp, signatureSet)
	if err != nil {
		return policyErr(errors.Wrapf(err, "validation of key %s (coll'%s':ns'%s') in tx %d:%d failed", key, coll, cc, blockNum, txNum))
	}

	p.SBEPChecked()

	return nil
}

func (p *baseEvaluatorAdapter) Evaluate(blockNum, txNum uint64, NsRwSets []*rwsetutil.NsRwSet, ns string, sd []*protoutil.SignedData) commonerrors.TxValidationError {
	// iterate over all writes in the rwset
	for _, nsRWSet := range NsRwSets {
		// skip other namespaces
		if nsRWSet.NameSpace != ns {
			continue
		}

		// public writes
		// we validate writes against key-level validation parameters
		// if any are present or the chaincode-wide endorsement policy
		for _, pubWrite := range nsRWSet.KvRwSet.Writes {
			err := p.checkSBAndCCEP(ns, "", pubWrite.Key, blockNum, txNum, sd)
			if err != nil {
				return err
			}
		}
		// public metadata writes
		// we validate writes against key-level validation parameters
		// if any are present or the chaincode-wide endorsement policy
		for _, pubMdWrite := range nsRWSet.KvRwSet.MetadataWrites {
			err := p.checkSBAndCCEP(ns, "", pubMdWrite.Key, blockNum, txNum, sd)
			if err != nil {
				return err
			}
		}
		// writes in collections
		// we validate writes against key-level validation parameters
		// if any are present or the chaincode-wide endorsement policy
		for _, collRWSet := range nsRWSet.CollHashedRwSets {
			coll := collRWSet.CollectionName
			for _, hashedWrite := range collRWSet.HashedRwSet.HashedWrites {
				key := string(hashedWrite.KeyHash)
				err := p.checkSBAndCCEP(ns, coll, key, blockNum, txNum, sd)
				if err != nil {
					return err
				}
			}
		}
		// metadata writes in collections
		// we validate writes against key-level validation parameters
		// if any are present or the chaincode-wide endorsement policy
		for _, collRWSet := range nsRWSet.CollHashedRwSets {
			coll := collRWSet.CollectionName
			for _, hashedMdWrite := range collRWSet.HashedRwSet.MetadataWrites {
				key := string(hashedMdWrite.KeyHash)
				err := p.checkSBAndCCEP(ns, coll, key, blockNum, txNum, sd)
				if err != nil {
					return err
				}
			}
		}
	}

	// we make sure that we check at least the CCEP to honour FAB-9473
	return p.CheckCCEPIfNoEPChecked(ns, blockNum, txNum, sd)
}

func (p *baseEvaluatorAdapter) EvaluateLtx(blockNum, txNum uint64, NsRwSets []*common.NsReadWriteSet, ns string, sd []*protoutil.SignedData) commonerrors.TxValidationError {
	// iterate over all writes in the rwset
	for _, nsRWSet := range NsRwSets {
		// skip other namespaces
		if nsRWSet.Namespace != ns {
			continue
		}

		// public writes
		// we validate writes against key-level validation parameters
		// if any are present or the chaincode-wide endorsement policy
		for _, pubWrite := range nsRWSet.Rwset.Writes {
			err := p.checkSBAndCCEP(ns, "", pubWrite.Key, blockNum, txNum, sd)
			if err != nil {
				return err
			}
		}
		// public metadata writes
		// we validate writes against key-level validation parameters
		// if any are present or the chaincode-wide endorsement policy
		for _, pubMdWrite := range nsRWSet.Rwset.MetadataWrites {
			err := p.checkSBAndCCEP(ns, "", pubMdWrite.Key, blockNum, txNum, sd)
			if err != nil {
				return err
			}
		}
		// writes in collections
		// we validate writes against key-level validation parameters
		// if any are present or the chaincode-wide endorsement policy
		for _, collRWSet := range nsRWSet.CollectionHashedRwset {
			coll := collRWSet.CollectionName
			for _, hashedWrite := range collRWSet.HashedRwset.HashedWrites {
				key := string(hashedWrite.KeyHash)
				err := p.checkSBAndCCEP(ns, coll, key, blockNum, txNum, sd)
				if err != nil {
					return err
				}
			}
		}
		// metadata writes in collections
		// we validate writes against key-level validation parameters
		// if any are present or the chaincode-wide endorsement policy
		for _, collRWSet := range nsRWSet.CollectionHashedRwset {
			coll := collRWSet.CollectionName
			for _, hashedMdWrite := range collRWSet.HashedRwset.MetadataWrites {
				key := string(hashedMdWrite.KeyHash)
				err := p.checkSBAndCCEP(ns, coll, key, blockNum, txNum, sd)
				if err != nil {
					return err
				}
			}
		}
	}

	// we make sure that we check at least the CCEP to honour FAB-9473
	return p.CheckCCEPIfNoEPChecked(ns, blockNum, txNum, sd)
}
