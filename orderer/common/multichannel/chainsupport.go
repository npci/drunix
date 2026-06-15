/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0

Modifications Copyright National Payments Corporation of India
*/

package multichannel

import (
	"github.com/VictoriaMetrics/fastcache"
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/npci/drunix/bccsp"
	"github.com/npci/drunix/common/ledger/blockledger"
	"github.com/npci/drunix/internal/pkg/identity"
	"github.com/npci/drunix/orderer/common/blockcutter"
	"github.com/npci/drunix/orderer/common/localconfig"
	"github.com/npci/drunix/orderer/common/msgprocessor"
	"github.com/npci/drunix/orderer/common/types"
	"github.com/npci/drunix/orderer/consensus"
	"github.com/npci/drunix/orderer/consensus/inactive"
	"github.com/npci/drunix/protoutil"
	"github.com/pkg/errors"
)

// ChainSupport holds the resources for a particular channel.
type ChainSupport struct {
	*ledgerResources
	msgprocessor.Processor
	*BlockWriter
	consensus.Chain
	cutter blockcutter.Receiver
	identity.SignerSerializer
	BCCSP bccsp.BCCSP

	// NOTE: It makes sense to add this to the ChainSupport since the design of Registrar does not assume
	// that there is a single consensus type at this orderer node and therefore the resolution of
	// the consensus type too happens only at the ChainSupport level.
	consensus.MetadataValidator

	// The registrar is not aware of the exact type that the Chain is, e.g. etcdraft, inactive, or follower.
	// Therefore, we let each chain report its cluster relation and status through this interface. Non cluster
	// type chains (solo, kafka) are assigned a static reporter.
	consensus.StatusReporter
	cacheStore *fastcache.Cache
}

func newChainSupport(
	registrar *Registrar,
	ledgerResources *ledgerResources,
	consenters map[string]consensus.Consenter,
	signer identity.SignerSerializer,
	blockcutterMetrics *blockcutter.Metrics,
	bccsp bccsp.BCCSP,
) (*ChainSupport, error) {
	// Read in the last block and metadata for the channel
	lastBlock := blockledger.GetBlock(ledgerResources, ledgerResources.Height()-1)
	metadata, err := protoutil.GetConsenterMetadataFromBlock(lastBlock)
	// Assuming a block created with cb.NewBlock(), this should not
	// error even if the orderer metadata is an empty byte slice
	if err != nil {
		return nil, errors.WithMessagef(err, "error extracting orderer metadata for channel: %s", ledgerResources.ConfigtxValidator().ChannelID())
	}

	// Construct limited support needed as a parameter for additional support
	cs := &ChainSupport{
		ledgerResources:  ledgerResources,
		SignerSerializer: signer,
		cutter: blockcutter.NewReceiverImpl(
			ledgerResources.ConfigtxValidator().ChannelID(),
			ledgerResources,
			blockcutterMetrics,
			registrar.maxBatchSize,
		),
		BCCSP: bccsp,
	}

	cs.cacheStore = fastcache.New(int(registrar.config.FileLedger.OrgBlockCacheSize))
	// Set up the msgprocessor
	cs.Processor = msgprocessor.NewStandardChannel(cs, msgprocessor.CreateStandardChannelFilters(cs, registrar.config), bccsp)

	// Set up the block writer
	cs.BlockWriter = newBlockWriter(lastBlock, registrar, cs)

	// Set up the consenter
	consenterType := ledgerResources.SharedConfig().ConsensusType()
	consenter, ok := consenters[consenterType]
	if !ok {
		return nil, errors.Errorf("error retrieving consenter of type: %s", consenterType)
	}

	cs.Chain, err = consenter.HandleChain(cs, metadata)
	if err != nil {
		return nil, errors.WithMessagef(err, "error creating consenter for channel: %s", cs.ChannelID())
	}

	cs.MetadataValidator, ok = cs.Chain.(consensus.MetadataValidator)
	if !ok {
		cs.MetadataValidator = consensus.NoOpMetadataValidator{}
	}

	cs.StatusReporter, ok = cs.Chain.(consensus.StatusReporter)
	if !ok { // Non-cluster types: solo, kafka
		cs.StatusReporter = consensus.StaticStatusReporter{ConsensusRelation: types.ConsensusRelationOther, Status: types.StatusActive}
	}

	clusterRelation, status := cs.StatusReporter.StatusReport()
	registrar.ReportConsensusRelationAndStatusMetrics(cs.ChannelID(), clusterRelation, status)

	logger.Debugf("[channel: %s] Done creating channel support resources", cs.ChannelID())

	return cs, nil
}

func (cs *ChainSupport) Reader() blockledger.Reader {
	return cs
}

// Signer returns the SignerSerializer for this channel.
func (cs *ChainSupport) Signer() identity.SignerSerializer {
	return cs
}

func (cs *ChainSupport) start() {
	cs.Chain.Start()
}

// BlockCutter returns the blockcutter.Receiver instance for this channel.
func (cs *ChainSupport) BlockCutter() blockcutter.Receiver {
	return cs.cutter
}

// Validate passes through to the underlying configtx.Validator
func (cs *ChainSupport) Validate(configEnv *cb.ConfigEnvelope) error {
	return cs.ConfigtxValidator().Validate(configEnv)
}

// ProposeConfigUpdate validates a config update using the underlying configtx.Validator
// and the consensus.MetadataValidator.
func (cs *ChainSupport) ProposeConfigUpdate(configtx *cb.Envelope) (*cb.ConfigEnvelope, error) {
	env, err := cs.ConfigtxValidator().ProposeConfigUpdate(configtx)
	if err != nil {
		return nil, err
	}

	bundle, err := cs.CreateBundle(cs.ChannelID(), env.Config)
	if err != nil {
		return nil, err
	}

	if err = checkResources(bundle); err != nil {
		return nil, errors.WithMessage(err, "config update is not compatible")
	}

	if err = cs.ValidateNew(bundle); err != nil {
		return nil, err
	}

	oldOrdererConfig, ok := cs.OrdererConfig()
	if !ok {
		logger.Panic("old config is missing orderer group")
	}

	// we can remove this check since this is being validated in checkResources earlier
	newOrdererConfig, ok := bundle.OrdererConfig()
	if !ok {
		return nil, errors.New("new config is missing orderer group")
	}

	if err = cs.ValidateConsensusMetadata(oldOrdererConfig, newOrdererConfig, false); err != nil {
		return nil, errors.WithMessage(err, "consensus metadata update for channel config update is invalid")
	}
	return env, nil
}

// ConfigProto passes through to the underlying configtx.Validator
func (cs *ChainSupport) ConfigProto() *cb.Config {
	return cs.ConfigtxValidator().ConfigProto()
}

// Sequence passes through to the underlying configtx.Validator
func (cs *ChainSupport) Sequence() uint64 {
	return cs.ConfigtxValidator().Sequence()
}

// Append appends a new block to the ledger in its raw form,
// unlike WriteBlock that also mutates its metadata.
func (cs *ChainSupport) Append(block *cb.Block) error {
	return cs.ledgerResources.ReadWriter.Append(block)
}

func newOnBoardingChainSupport(
	ledgerResources *ledgerResources,
	config localconfig.TopLevel,
	bccsp bccsp.BCCSP,
) (*ChainSupport, error) {
	cs := &ChainSupport{ledgerResources: ledgerResources}
	cs.Processor = msgprocessor.NewStandardChannel(cs, msgprocessor.CreateStandardChannelFilters(cs, config), bccsp)
	cs.Chain = &inactive.Chain{Err: errors.New("system channel creation pending: server requires restart")}
	cs.StatusReporter = consensus.StaticStatusReporter{ConsensusRelation: types.ConsensusRelationConsenter, Status: types.StatusInactive}

	logger.Debugf("[channel: %s] Done creating onboarding channel support resources", cs.ChannelID())

	return cs, nil
}

func (cs *ChainSupport) AppendOrgMerkleInfo(key []byte, orgMerkleInfo []byte) error {
	cs.cacheStore.SetBig(key, orgMerkleInfo)
	fl := cs.ledgerResources.ReadWriter.(blockledger.SparseMetadataReadWriter)
	return fl.AppendOrgMerkleInfo(key, orgMerkleInfo)
}

func (cs *ChainSupport) AppendOrgBlockIndex(key []byte, orgBlockIndex []byte) error {
	cs.cacheStore.SetBig(key, orgBlockIndex)
	fl := cs.ledgerResources.ReadWriter.(blockledger.SparseMetadataReadWriter)
	return fl.AppendOrgBlockIndex(key, orgBlockIndex)
}

func (cs *ChainSupport) SaveChannelHeadInfo(key []byte, channelHeadInfo []byte) error {
	cs.cacheStore.SetBig(key, channelHeadInfo)
	fl := cs.ledgerResources.ReadWriter.(blockledger.SparseMetadataReadWriter)
	return fl.SaveChannelHeadInfo(key, channelHeadInfo)
}

func (cs *ChainSupport) GetOrgMetaValue(key []byte) ([]byte, error) {
	value := cs.cacheStore.GetBig(nil, key)
	if value != nil {
		return value, nil
	}
	fl := cs.ledgerResources.ReadWriter.(blockledger.SparseMetadataReadWriter)
	return fl.GetOrgMetaValue(key)
}

func (cs *ChainSupport) GetSparseChannel() (chan *cb.Block, error) { //spbc
	fl := cs.ledgerResources.ReadWriter.(blockledger.SparseMetadataReadWriter)
	return fl.GetSparseChannel()
}

// AppendSavePoint implements blockledger.SparseMetadataReadWriter.
func (cs *ChainSupport) AppendSavePoint(key []byte, num []byte) error {
	fl := cs.ledgerResources.ReadWriter.(blockledger.SparseMetadataReadWriter)
	return fl.AppendSavePoint(key, num)
}

// GetSavePoint implements blockledger.SparseMetadataReadWriter.
func (cs *ChainSupport) GetSavePoint(key []byte) ([]byte, error) {
	fl := cs.ledgerResources.ReadWriter.(blockledger.SparseMetadataReadWriter)
	return fl.GetSavePoint(key)
}
