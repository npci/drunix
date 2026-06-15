/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/
package plugindispatcher

import (
	"fmt"
	"sync"

	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/npci/drunix/common/policies"
	txvalidatorplugin "github.com/npci/drunix/core/committer/txvalidator/plugin"
	validation "github.com/npci/drunix/core/handlers/validation/api"
	vc "github.com/npci/drunix/core/handlers/validation/api/capabilities"
	"github.com/npci/drunix/core/policy"
	"github.com/npci/drunix/msp"
	"github.com/pkg/errors"
)

type PluginValidatorAdapter struct {
	*PluginValidator
	pluginChannelMapping map[txvalidatorplugin.Name]*pluginsByChannelAdapter
	txvalidatorplugin.MapperAdapter
}

type ContextAdapter struct {
	Seq        int
	Envelope   *common.Envelope
	TxID       string
	Channel    string
	PluginName string
	Policy     []byte
	Namespace  string
	BlockNum   uint64
}

// String returns a string representation of this Context.
func (c ContextAdapter) String() string {
	return fmt.Sprintf("Tx %s, seq %d in block %d for channel %s with validation plugin %s", c.TxID, c.Seq, c.BlockNum, c.Channel, c.PluginName)
}

func NewPluginValidatorAdapter(pm txvalidatorplugin.MapperAdapter, qec QueryExecutorCreator, deserializer msp.IdentityDeserializer, capabilities vc.Capabilities, cpmg policies.ChannelPolicyManagerGetter, cor CollectionResources) *PluginValidatorAdapter {
	return &PluginValidatorAdapter{
		PluginValidator: &PluginValidator{
			capabilities: capabilities,
			// Mapper:                     pm,
			QueryExecutorCreator:       qec,
			IdentityDeserializer:       deserializer,
			ChannelPolicyManagerGetter: cpmg,
			CollectionResources:        cor,
		},
		pluginChannelMapping: make(map[txvalidatorplugin.Name]*pluginsByChannelAdapter),
		MapperAdapter:        pm,
	}
}

func (pv *PluginValidatorAdapter) ValidateWithPlugin(ctx *ContextAdapter) error {
	plugin, err := pv.getOrCreatePlugin(ctx)
	if err != nil {
		return &validation.ExecutionFailureError{
			Reason: fmt.Sprintf("plugin with name %s couldn't be used: %v", ctx.PluginName, err),
		}
	}

	err = plugin.Validate(ctx.BlockNum, ctx.Envelope, ctx.Namespace, ctx.Seq, 0, txvalidatorplugin.SerializedPolicy(ctx.Policy))
	validityStatus := "valid"
	if err != nil {
		validityStatus = fmt.Sprintf("invalid: %v", err)
	}
	logger.Debug("Transaction", ctx.TxID, "appears to be", validityStatus)
	return err
}

func (pv *PluginValidatorAdapter) ValidateWithPluginLtx(ctx *ContextAdapter) error {
	plugin, err := pv.getOrCreatePlugin(ctx)
	if err != nil {
		return &validation.ExecutionFailureError{
			Reason: fmt.Sprintf("plugin with name %s couldn't be used: %v", ctx.PluginName, err),
		}
	}

	err = plugin.ValidateLtx(ctx.BlockNum, ctx.Envelope, ctx.Namespace, ctx.Seq, 0, txvalidatorplugin.SerializedPolicy(ctx.Policy))
	validityStatus := "valid"
	if err != nil {
		validityStatus = fmt.Sprintf("invalid: %v", err)
	}
	logger.Debug("Transaction", ctx.TxID, "appears to be", validityStatus)
	return err
}

func (pv *PluginValidatorAdapter) getOrCreatePlugin(ctx *ContextAdapter) (validation.PluginAdapter, error) {

	pluginFactory := pv.FactoryByName(txvalidatorplugin.Name(ctx.PluginName))
	if pluginFactory == nil {
		return nil, errors.Errorf("plugin with name %s wasn't found", ctx.PluginName)
	}

	pluginsByChannel := pv.getOrCreatePluginChannelMapping(txvalidatorplugin.Name(ctx.PluginName), pluginFactory)
	return pluginsByChannel.createPluginIfAbsent(ctx.Channel)
}

func (pv *PluginValidatorAdapter) getOrCreatePluginChannelMapping(plugin txvalidatorplugin.Name, pf validation.PluginFactoryAdapter) *pluginsByChannelAdapter {
	pv.Lock()
	defer pv.Unlock()
	endorserChannelMapping, exists := pv.pluginChannelMapping[txvalidatorplugin.Name(plugin)]
	if !exists {
		endorserChannelMapping = &pluginsByChannelAdapter{
			pluginFactory:    pf,
			channels2Plugins: make(map[string]validation.PluginAdapter),
			pv:               pv,
		}
		pv.pluginChannelMapping[txvalidatorplugin.Name(plugin)] = endorserChannelMapping
	}
	return endorserChannelMapping
}

type pluginsByChannelAdapter struct {
	sync.RWMutex
	pluginFactory    validation.PluginFactoryAdapter
	channels2Plugins map[string]validation.PluginAdapter
	pv               *PluginValidatorAdapter
}

func (pbc *pluginsByChannelAdapter) createPluginIfAbsent(channel string) (validation.PluginAdapter, error) {
	pbc.RLock()
	plugin, exists := pbc.channels2Plugins[channel]
	pbc.RUnlock()
	if exists {
		return plugin, nil
	}

	pbc.Lock()
	defer pbc.Unlock()
	plugin, exists = pbc.channels2Plugins[channel]
	if exists {
		return plugin, nil
	}

	pluginInstance := pbc.pluginFactory.New()
	plugin, err := pbc.initPlugin(pluginInstance, channel)
	if err != nil {
		return nil, err
	}
	pbc.channels2Plugins[channel] = plugin
	return plugin, nil
}

func (pbc *pluginsByChannelAdapter) initPlugin(plugin validation.PluginAdapter, channel string) (validation.PluginAdapter, error) {
	pp, err := policy.New(pbc.pv.IdentityDeserializer, channel, pbc.pv.ChannelPolicyManagerGetter)
	if err != nil {
		return nil, errors.WithMessage(err, "could not obtain a policy evaluator")
	}

	pe := &PolicyEvaluatorWrapper{IdentityDeserializer: pbc.pv.IdentityDeserializer, PolicyEvaluator: pp}
	sf := &StateFetcherImpl{QueryExecutorCreator: pbc.pv}
	if err := plugin.Init(pe, sf, pbc.pv.capabilities, pbc.pv.CollectionResources); err != nil {
		return nil, errors.Wrap(err, "failed initializing plugin")
	}
	return plugin, nil
}
