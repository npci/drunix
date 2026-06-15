/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/
/*
DRUNIX
*/
package sparseblock

import (
	"math"

	"github.com/VictoriaMetrics/fastcache"
	ab "github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/npci/drunix/common/deliver"
	"github.com/npci/drunix/common/flogging"
	"github.com/npci/drunix/common/ledger/blockledger"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/statedb/ordererstatedb"
	"github.com/npci/drunix/orderer/common/localconfig"
	"github.com/npci/drunix/orderer/common/multichannel"
)

var logger = flogging.MustGetLogger("common.sparseblock")

type OrgChainsRegistrar struct { // parent of all channels
	ordererChannelMap map[string]*ChannelOrgChains
	ordererRegistrar  *multichannel.Registrar
	fileLedgerConf    localconfig.FileLedger
	mvccApplicable    bool
}

type ChannelOrgChains struct {
	channelID    string
	orgChainMap  map[string]*OrgChain
	chainSupport *multichannel.ChainSupport //reading the fat blocks - fat blockprocessor
	versionDB    ordererstatedb.OrdererDBHandler
}

// NewOrgChainsRegistrar creates and returns an OrgChainsRegistrar
func NewOrgChainsRegistrar(fatBlockRegistrar *multichannel.Registrar,
	fileLedgerConf localconfig.FileLedger, mvccApplicable bool) *OrgChainsRegistrar {
	return &OrgChainsRegistrar{
		ordererChannelMap: make(map[string]*ChannelOrgChains),
		ordererRegistrar:  fatBlockRegistrar,
		fileLedgerConf:    fileLedgerConf,
		mvccApplicable:    mvccApplicable,
	}
}

// buildChannelTree returns a ChannelOrgChains for the given channelID, initializing it if necessary
// It handles the creation of ordererDBHandler and starts processing fat blocks in routines
func (orgRegistrar *OrgChainsRegistrar) buildChannelTree(kVersionDBStore *ordererstatedb.KVDBProvider, channelID string) *ChannelOrgChains {
	channelOrgChains, ok := orgRegistrar.ordererChannelMap[channelID]
	if !ok {
		orgMap := make(map[string]*OrgChain)
		chainSupport := orgRegistrar.ordererRegistrar.GetChain(channelID)
		ordererDBHandler, err := kVersionDBStore.NewOrdererDBHandler(channelID)
		if err != nil {
			logger.Errorf("error while getting ordererleveldb instance :%v", err)
		}

		channelOrgChains = &ChannelOrgChains{
			channelID:    channelID,
			orgChainMap:  orgMap,
			chainSupport: chainSupport,
			versionDB:    ordererDBHandler,
		}
		go processFatBlocks(channelOrgChains, orgRegistrar.fileLedgerConf.KVStoreCacheSize, orgRegistrar.mvccApplicable)
	}

	return channelOrgChains
}

// TODO: all these envs has to be passed as a part of config
// buildAndProcessBigChainTree initializes the KVDBProvider and processes channels
// in an infinite loop to buildChannelTree
func (orgRegistrar *OrgChainsRegistrar) buildAndProcessBigChainTree() {
	kVersionDBProvider, err := ordererstatedb.NewKVDBProvider(orgRegistrar.fileLedgerConf.KVStore)
	if err != nil {
		logger.Errorf("error while getting ordererleveldb instance :%v", err)
	}
	// FIXME: review this infinite loop
	// This variable basically checks whether channel is present in the orderer
	channelExists := false
	for {
		for _, info := range orgRegistrar.ordererRegistrar.ChannelList().Channels {
			channelExists = true
			orgRegistrar.ordererChannelMap[info.Name] = orgRegistrar.buildChannelTree(kVersionDBProvider, info.Name)
		}
		/*
			DRUNIX: this channel detects that new channel is added and releases the loop,
				   till that moment it will block
		*/
		if channelExists {
			orgRegistrar.ordererRegistrar.ReleaseRegistrarChannel()
		}
	}
}

// InitializeSparseChains starts the process of building and processing big chain trees in a separate goroutine
func (orgRegistrar *OrgChainsRegistrar) InitializeSparseChains() {
	go orgRegistrar.buildAndProcessBigChainTree()
}

// GetChain returns the deliver-Chain for the given channelID and mspID,
// initializing it if not found in the channel map and storing into the channelMap
func (ocRegistrar *OrgChainsRegistrar) GetChain(channelID string, mspID string) deliver.Chain {

	channelsMap, ok := ocRegistrar.ordererChannelMap[channelID]

	if !ok {
		return nil
	} else {
		orgChain, ok := channelsMap.orgChainMap[mspID]
		if !ok {
			chainSupport := ocRegistrar.ordererRegistrar.GetChain(channelID)
			orgChain = NewOrgChain(mspID, channelID, chainSupport, ocRegistrar.fileLedgerConf.OrgBlockCacheSize)
			channelsMap.orgChainMap[mspID] = orgChain
		}
		return orgChain
	}
}

// getFatIterator returns an iterator and starting blockNum for the given chain and blockNum
func getFatIterator(chain deliver.Chain, n uint64) (blockledger.Iterator, uint64) {
	seekS := &ab.SeekSpecified{Number: n}
	SeekPositionS := ab.SeekPosition_Specified{Specified: seekS}
	sp := ab.SeekPosition{Type: &SeekPositionS}
	return chain.Reader().Iterator(&sp)
}

// NewOrgChain creates orgChain for a newly added org to the channel
func NewOrgChain(mspID string, channel string, chainSupport deliver.Chain, orgBlockCacheSize uint64) *OrgChain {
	store := fastcache.New(int(orgBlockCacheSize))

	return &OrgChain{
		mspID:        mspID,
		oCStore:      newOrgChainStore(mspID, channel, store, chainSupport),
		chainSupport: chainSupport,
	}
}

// newOrgChainStore creates and returns an OrgChainStore
// Initializes store, mspId, channelId, and readers
func newOrgChainStore(mspID, channelId string, store *fastcache.Cache, chainSupport deliver.Chain) *OrgChainStore {
	sparseMetadataRWriter := getSparseMetadataReadWriter(chainSupport)

	/*
		DRUNIX: savepoint of orderer which is the last vanilla block number which came to the network
			   if it does not exist we set it to maxUint value
	*/
	var ordererSavepoint uint64
	ordererSavepoint = math.MaxUint64

	/*
		lastProcessedOrdBlkBytes, err := sparseMetadataRWriter.GetSavePoint(fmt.Appendf(nil, "savepoint-%v", channelId))
		if err != nil {
			logger.Errorf("Not found lastProcessedOrdBlk in the sparseMetadataReadWriter-----------")
		}

		// Handle the case where lastProcessedOrdBlkBytes is nil
		if len(lastProcessedOrdBlkBytes) != 0 {
			ordererSavepoint = binary.BigEndian.Uint64(lastProcessedOrdBlkBytes)
		}
	*/

	return &OrgChainStore{
		ordererSavePoint:      ordererSavepoint,
		store:                 store,
		mspId:                 mspID,
		channelId:             channelId,
		fatBlockChainReader:   chainSupport.Reader(),
		sparseBlockReadWriter: sparseMetadataRWriter,
	}
}
