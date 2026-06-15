/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
Modifications Copyright National Payments Corporation of India
*/
package statebased

import (
	validation "github.com/npci/drunix/core/handlers/validation/api/policies"
	s "github.com/npci/drunix/core/handlers/validation/api/state"
)

// NewV20Evaluator returns a policy evaluator that checks
// 3 kinds of policies:
// 1) chaincode endorsement policies;
// 2) state-based endorsement policies;
// 3) collection-level endorsement policies.
func NewV20EvaluatorAdapter(
	vpmgr KeyLevelValidationParameterManager,
	policySupport validation.PolicyEvaluator,
	collRes CollectionResources,
	StateFetcher s.StateFetcher,
) *policyCheckerFactoryV20Adapter {
	return &policyCheckerFactoryV20Adapter{
		&policyCheckerFactoryV20{
			vpmgr:         vpmgr,
			policySupport: policySupport,
			StateFetcher:  StateFetcher,
			collRes:       collRes,
		},
	}
}

type policyCheckerFactoryV20Adapter struct {
	*policyCheckerFactoryV20
}

func (p *policyCheckerFactoryV20Adapter) Evaluator(ccEP []byte) RWSetPolicyEvaluator {

	return &baseEvaluatorAdapter{
		baseEvaluator: &baseEvaluator{
			epEvaluator: &policyCheckerV20{
				ccEP:          ccEP,
				policySupport: p.policySupport,
				nsEPChecked:   map[string]bool{},
				collRes:       p.collRes,
				StateFetcher:  p.StateFetcher,
			},
			vpmgr:         p.vpmgr,
			policySupport: p.policySupport,
		},
	}
}
