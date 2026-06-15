/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/

package peer

import (
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/npci/drunix/bccsp"
	"github.com/npci/drunix/common/channelconfig"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/statedb"
	mspmgmt "github.com/npci/drunix/msp/mgmt"
)

func (p *Peer) InitializeChannel(channelID string, cryptoProvider bccsp.BCCSP, dbProvider statedb.VersionedDB) (*Channel, error) {

	chanConf, err := fetchChannelConfig(dbProvider)
	if err != nil {
		return nil, err
	}

	bundle, err := channelconfig.NewBundle(channelID, chanConf, cryptoProvider)
	if err != nil {
		return nil, err
	}

	leanEnabled := false
	appConfig, exist := bundle.ApplicationConfig()
	if exist {
		leanEnabled = appConfig.Capabilities().LeanFormatEnabled()
		p.ServerConfig.Logger.Infof("Lean Format Enabled : %v", leanEnabled)
	}

	capabilitiesSupportedOrPanic(bundle)

	channelconfig.LogSanityChecks(bundle)

	mspCallback := func(bundle *channelconfig.Bundle) {
		// TODO remove once all references to mspmgmt are gone from peer code
		mspmgmt.XXXSetMSPManager(channelID, bundle.MSPManager())
	}

	channel := &Channel{
		id:             channelID,
		resources:      bundle,
		cryptoProvider: cryptoProvider,
		LeanEnabled:    leanEnabled,
	}

	callbacks := []channelconfig.BundleActor{
		mspCallback,
	}
	callbacks = append(callbacks, p.configCallbacks...)

	channel.bundleSource = channelconfig.NewBundleSource(
		bundle,
		callbacks...,
	)

	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.channels == nil {
		p.channels = map[string]*Channel{}
	}
	p.channels[channelID] = channel

	return channel, nil
}

func fetchChannelConfig(dbProvider statedb.VersionedDB) (*common.Config, error) {

	versionedValue, err := dbProvider.GetState(peerNamespace, channelConfigKey)
	if err != nil {
		return nil, err
	}
	if versionedValue == nil || versionedValue.Value == nil {
		return nil, nil
	}
	configEnvelope := &common.ConfigEnvelope{}
	if err := proto.Unmarshal(versionedValue.Value, configEnvelope); err != nil {
		return nil, err
	}
	return configEnvelope.Config, nil
}
