/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/

/*
DRUNIX
*/
package sparseblock

import (
	"encoding/binary"
	"fmt"

	"github.com/npci/drunix/common/deliver"
	"github.com/npci/drunix/common/ledger/blockledger"
	"github.com/npci/drunix/common/policies"
	"github.com/npci/drunix/orderer/common/multichannel"
	"github.com/npci/drunix/protoutil"
	"github.com/VictoriaMetrics/fastcache"
	proto "github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	ab "github.com/hyperledger/fabric-protos-go/orderer"
)

type OrgChain struct { //individual org specific chain within a channel
	oCStore      *OrgChainStore //where the org blocks of a specific org are stored
	chainSupport deliver.Chain
	mspID        string
}

type OrgChainStore struct {
	store                 *fastcache.Cache
	fatBlockChainReader   blockledger.Reader
	sparseBlockReadWriter blockledger.SparseMetadataReadWriter
	mspId                 string
	channelId             string
	ordererSavePoint      uint64
}

type orgCacheIterator struct {
	ledger               OrgChainReadWriter
	fatBlockChainReader  blockledger.Reader
	mgr                  *OrgChainStore
	blockNumber          uint64
	maxAvailableBlockNum uint64
}

// Iterator returns an iterator and starting block number based on the
// seek position (Oldest, Newest, Specified, NextCommit)
func (ocStore *OrgChainStore) Iterator(seekPosition *ab.SeekPosition) (blockledger.Iterator, uint64) {

	var startingBlockNumber uint64
	switch start := seekPosition.Type.(type) {
	case *ab.SeekPosition_Oldest:
		startingBlockNumber = 0 // first orgBlock
	case *ab.SeekPosition_Newest:
		startingBlockNumber = ocStore.Height() - 1
	case *ab.SeekPosition_Specified:
		startingBlockNumber = start.Specified.Number
		height := ocStore.Height()
		if startingBlockNumber > height {
			return &blockledger.NotFoundErrorIterator{}, 0
		}
	case *ab.SeekPosition_NextCommit:
		startingBlockNumber = ocStore.Height()
	default:
		return &blockledger.NotFoundErrorIterator{}, 0
	}

	return &orgCacheIterator{
		ledger:               ocStore,
		blockNumber:          startingBlockNumber,
		maxAvailableBlockNum: ocStore.Height() - 1,
		mgr:                  ocStore,
	}, startingBlockNumber
}

// Retrieves the height of the org chain by reading the channel header metadata
func (ocStore *OrgChainStore) Height() uint64 {
	var chHeader ChannelHeadInfoProto
	var ChannelMetadataBytes []byte
	var err error
	var ok bool
	cHeaderContainerMutex.RLock()
	ChannelMetadataBytes, ok = channelHeaderContainer[fmt.Sprint(ocStore.channelId, ".ChannelHeadInfo")]
	cHeaderContainerMutex.RUnlock()
	if !ok {
		ChannelMetadataBytes, err = ocStore.sparseBlockReadWriter.GetOrgMetaValue([]byte(fmt.Sprint(ocStore.channelId, ".ChannelHeadInfo")))
		if err != nil {
			logger.Info("error while getting channelMetadataBytes")
		}
	}
	if ChannelMetadataBytes == nil {
		logger.Warn("ChannelHeader is nil")
	}

	err = proto.Unmarshal(ChannelMetadataBytes, &chHeader)
	if err != nil {
		logger.Info("will be processing zeroth block")
		panic("Unable to unmarshal channel header value in sparse blocks metadata space")
	}

	headPosition, ok := chHeader.OrgHeadPostion[ocStore.mspId]
	if !ok {
		return 0
	}

	return headPosition.LastOrgBlockNum + 1
}

// RetrieveBlockByNumber returns the block for the given block number
func (ocStore *OrgChainStore) RetrieveBlockByNumber(blockNumber uint64) (*cb.Block, error) {
	return retriveOrgBlockByNum(blockNumber, ocStore.store, ocStore.fatBlockChainReader,
		ocStore.sparseBlockReadWriter, ocStore.mspId, ocStore.channelId, ocStore.ordererSavePoint)
}

func (ocStore *OrgChainStore) Get(key []byte) ([]byte, bool) {

	/*
		DRUNIX
		If cache is evicted due to memory limit, still key and nil value persists on fast cache.
		So checking the length of data to confirm
	*/

	// if ocStore.store.Has(key) {
	// 	return ocStore.store.GetBig(nil, key), true
	// }
	if blockBytes := ocStore.store.GetBig(nil, key); len(blockBytes) != 0 {
		return blockBytes, true
	}
	blockNumKey := binary.BigEndian.Uint64(key)
	block, err := ocStore.RetrieveBlockByNumber(blockNumKey)
	blockBytes := protoutil.MarshalOrPanic(block)
	return blockBytes, err == nil
}

func (ocStore *OrgChainStore) Put(key, value []byte) {
	ocStore.store.SetBig(key, value)
}

// Close implements blockledger.Iterator.
func (oci *orgCacheIterator) Close() {
	logger.Info("Unimplemented Close has been called!")
}

// waitForBlock waits until the blockNum is reached and returns one less than the current height
func (oci *orgCacheIterator) waitForBlock(blockNum uint64) uint64 {
	logger.Infof("going to wait for blocknumber : %v, for org: %v", blockNum, oci.mgr.mspId)
	for oci.mgr.Height() <= blockNum {

		/*
			DRUNIX
			removed frequent for loop for checking the block height
			and replaced the block height check with sync.Cond
		*/
		channelHeaderContainerCond.L.Lock()
		channelHeaderContainerCond.Wait()
		channelHeaderContainerCond.L.Unlock()
	}

	return oci.mgr.Height() - 1
}

// Next implements blockledger.Iterator.
func (oci *orgCacheIterator) Next() (*cb.Block, cb.Status) {

	blockNumberbytes := make([]byte, 8)
	binary.BigEndian.PutUint64(blockNumberbytes, oci.blockNumber)
	if oci.maxAvailableBlockNum < oci.blockNumber {
		oci.maxAvailableBlockNum = oci.waitForBlock(oci.blockNumber)
	}
	/*
		DRUNIX
		If the block is not there in cache as well as FileStore we will throw error
	*/
	var result []byte
	var ok bool
	result, ok = oci.ledger.Get(blockNumberbytes)
	if !ok || result == nil || len(result) == 0 {
		logger.Errorf("Next block not found")
		return nil, cb.Status_SERVICE_UNAVAILABLE
	}

	oci.blockNumber += 1
	block := protoutil.UnmarshalBlockOrPanic(result)
	logger.Infof("returning Next blocknumber :%v, for org: %v", oci.blockNumber, oci.mgr.mspId)

	return block, cb.Status_SUCCESS
}

func searchAndRetreiveFatBlockMerkleInfo(orgblockNumber uint64, orgId, channelId string, sparseMetadataReadWriter blockledger.SparseMetadataReadWriter) (FatBlockMerkleInfoProto, uint64, error) {

	fatBlockNumberBytes, err := sparseMetadataReadWriter.GetOrgMetaValue([]byte(fmt.Sprintf("%v.%v.%v", channelId, orgId, orgblockNumber)))
	if err != nil || fatBlockNumberBytes == nil {
		return FatBlockMerkleInfoProto{}, 0, err
	}

	fatBlockNumber := binary.BigEndian.Uint64(fatBlockNumberBytes)
	fatBlockMerkleInfoBytes, err := sparseMetadataReadWriter.GetOrgMetaValue([]byte(fmt.Sprint(channelId, ".", fatBlockNumber)))
	if err != nil {
		return FatBlockMerkleInfoProto{}, fatBlockNumber, err
	}

	var fatBlockMerkleInfo FatBlockMerkleInfoProto

	err = proto.Unmarshal(fatBlockMerkleInfoBytes, &fatBlockMerkleInfo)
	if err != nil {
		logger.Panicf("error while unmarshalling: %v", err)
		return FatBlockMerkleInfoProto{}, fatBlockNumber, err
	}

	return fatBlockMerkleInfo, fatBlockNumber, nil
}

// Retrieves an orgBlock by block number, checking cache first and assembling block if not found
func retriveOrgBlockByNum(blockNum uint64, store *fastcache.Cache, fatBlockReader blockledger.Reader,
	sparseMetadataReadWriter blockledger.SparseMetadataReadWriter, orgId string, channelId string, ordererSavepoint uint64) (*cb.Block, error) {
	blockNumberBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(blockNumberBytes, blockNum)
	/*
		DRUNIX
		If cache is evicted due to memory limit, still key and nil value persists on fast cache.
		So checking the length of data to confirm
	*/
	if blockBytes := store.GetBig(nil, blockNumberBytes); len(blockBytes) != 0 {
		return protoutil.UnmarshalBlockOrPanic(blockBytes), nil
	}

	// ***********************************************

	lastProcessedOrdBlkBytes, err := sparseMetadataReadWriter.GetSavePoint([]byte(fmt.Sprintf("savepoint-%v", channelId)))
	if err != nil {
		logger.Errorf("Not found lastProcessedOrdBlk in the sparseMetadataReadWriter-----------")
	}

	var lastProcessedOrdBlk uint64
	if (lastProcessedOrdBlkBytes) != nil {
		lastProcessedOrdBlk = binary.BigEndian.Uint64(lastProcessedOrdBlkBytes)
	}

	if blockNum <= lastProcessedOrdBlk {
		logger.Warnf("Retrieving the block from fatBlock Reader-----------")
		return fatBlockReader.RetrieveBlockByNumber(blockNum)
	}

	// ***********************************************
	fatBlockMerkleInfo, fatBlockNumber, err := searchAndRetreiveFatBlockMerkleInfo(blockNum, orgId, channelId, sparseMetadataReadWriter)
	if err != nil {
		return nil, err
	}
	// block is out of cache we need to assemble it
	block := assembleOrgBlock(fatBlockMerkleInfo, orgId, fatBlockNumber, fatBlockReader)

	return block, nil
}

func (oc *OrgChain) Sequence() uint64 {
	return oc.chainSupport.Sequence()
}

func (oc *OrgChain) PolicyManager() policies.Manager {
	return oc.chainSupport.PolicyManager()
}

func (oc *OrgChain) Errored() <-chan struct{} { //this needs to be handled with clarity
	return oc.chainSupport.Errored()
}

func (oc *OrgChain) Reader() blockledger.Reader {
	return oc.oCStore
}

func getSparseMetadataReadWriter(chain deliver.Chain) blockledger.SparseMetadataReadWriter {
	cs := chain.(*multichannel.ChainSupport)
	return cs
}
