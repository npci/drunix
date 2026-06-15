/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
Modifications Copyright National Payments Corporation of India
*/
package validation

import "github.com/hyperledger/fabric-protos-go/common"

// Plugin validates transactions
type PluginAdapter interface {
	// Validate returns nil if the action at the given position inside the transaction
	// at the given position in the given block is valid, or an error if not.
	Validate(blockNum uint64, envelope *common.Envelope, namespace string, txPosition int, actionPosition int, contextData ...ContextDatum) error
	ValidateLtx(blockNum uint64, envelope *common.Envelope, namespace string, txPosition int, actionPosition int, contextData ...ContextDatum) error

	// Init injects dependencies into the instance of the Plugin
	Init(dependencies ...Dependency) error
}

// PluginFactory creates a new instance of a Plugin
type PluginFactoryAdapter interface {
	New() PluginAdapter
}
