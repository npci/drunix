/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
Modifications Copyright National Payments Corporation of India
*/
package fileledger

import (
	cb "github.com/hyperledger/fabric-protos-go/common"
	ab "github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/npci/drunix/common/flogging"
	"github.com/npci/drunix/common/ledger"
	"github.com/npci/drunix/common/ledger/blockledger"
)

var (
	logger               = flogging.MustGetLogger("common.ledger.blockledger.file")
	sparseChanBufferSize = 2
)

// FileLedger is a struct used to interact with a node's ledger
type FileLedger struct {
	blockStore FileLedgerBlockStore
	signal     chan struct{}
	sparseCh   chan *cb.Block
}

// FileLedgerBlockStore defines the interface to interact with deliver when using a
// file ledger
type FileLedgerBlockStore interface {
	AddBlock(block *cb.Block) error
	GetBlockchainInfo() (*cb.BlockchainInfo, error)
	RetrieveBlocks(startBlockNumber uint64) (ledger.ResultsIterator, error)
	Shutdown()
	RetrieveBlockByNumber(blockNum uint64) (*cb.Block, error)
}

type FileLedgerOrgStore interface {
	AppendOrgBlockIndex(key []byte, orgVal []byte) error
	AppendOrgMerkleInfo(key []byte, orgMerkleInfo []byte) error
	SaveChannelHeadInfo(key []byte, channelHeadInfo []byte) error
	GetOrgMetaValue(key []byte) ([]byte, error)
	AppendSavePoint(key []byte, num []byte) error
	GetSavePoint(key []byte) ([]byte, error)
}

// NewFileLedger creates a new FileLedger for interaction with the ledger
func NewFileLedger(blockStore FileLedgerBlockStore) *FileLedger {
	return &FileLedger{
		blockStore: blockStore,
		/*
			DRUNIX: added sparseCh to store blocks and retrieve blocks in the sparse module
		*/
		sparseCh: make(chan *cb.Block, sparseChanBufferSize),
		signal:   make(chan struct{}),
	}
}

type fileLedgerIterator struct {
	ledger         *FileLedger
	blockNumber    uint64
	commonIterator ledger.ResultsIterator
}

// Next blocks until there is a new block available, or until Close is called.
// It returns an error if the next block is no longer retrievable.
func (i *fileLedgerIterator) Next() (*cb.Block, cb.Status) {
	result, err := i.commonIterator.Next()
	if err != nil {
		logger.Error(err)
		return nil, cb.Status_SERVICE_UNAVAILABLE
	}
	// Cover the case where another thread calls Close on the iterator.
	if result == nil {
		return nil, cb.Status_SERVICE_UNAVAILABLE
	}
	return result.(*cb.Block), cb.Status_SUCCESS
}

// Close releases resources acquired by the Iterator
func (i *fileLedgerIterator) Close() {
	i.commonIterator.Close()
}

// Iterator returns an Iterator, as specified by an ab.SeekInfo message, and its
// starting block number
func (fl *FileLedger) Iterator(startPosition *ab.SeekPosition) (blockledger.Iterator, uint64) {
	var startingBlockNumber uint64
	switch start := startPosition.Type.(type) {
	case *ab.SeekPosition_Oldest:
		startingBlockNumber = 0
	case *ab.SeekPosition_Newest:
		info, err := fl.blockStore.GetBlockchainInfo()
		if err != nil {
			logger.Panic(err)
		}
		newestBlockNumber := info.Height - 1
		if info.BootstrappingSnapshotInfo != nil && newestBlockNumber == info.BootstrappingSnapshotInfo.LastBlockInSnapshot {
			newestBlockNumber = info.Height
		}
		startingBlockNumber = newestBlockNumber
	case *ab.SeekPosition_Specified:
		startingBlockNumber = start.Specified.Number
		height := fl.Height()
		if startingBlockNumber > height {
			return &blockledger.NotFoundErrorIterator{}, 0
		}
	case *ab.SeekPosition_NextCommit:
		startingBlockNumber = fl.Height()
	default:
		return &blockledger.NotFoundErrorIterator{}, 0
	}

	iterator, err := fl.blockStore.RetrieveBlocks(startingBlockNumber)
	if err != nil {
		logger.Warnw("Failed to initialize block iterator", "blockNum", startingBlockNumber, "error", err)
		return &blockledger.NotFoundErrorIterator{}, 0
	}

	return &fileLedgerIterator{ledger: fl, blockNumber: startingBlockNumber, commonIterator: iterator}, startingBlockNumber
}

// Height returns the number of blocks on the ledger
func (fl *FileLedger) Height() uint64 {
	info, err := fl.blockStore.GetBlockchainInfo()
	if err != nil {
		logger.Panic(err)
	}
	return info.Height
}

// Append a new block to the ledger
func (fl *FileLedger) Append(block *cb.Block) error {
	err := fl.blockStore.AddBlock(block)
	if err == nil {
		close(fl.signal)
		fl.signal = make(chan struct{})
		logger.Info("Going to put the fat block into the channel") //spbc
		fl.sparseCh <- block
	}
	return err
}

func (fl *FileLedger) RetrieveBlockByNumber(blockNumber uint64) (*cb.Block, error) {
	return fl.blockStore.RetrieveBlockByNumber(blockNumber)
}

/*
DRUNIX: AppendOrgMerkleInfo adds org-specific Merkle info to the block store
*/
func (fl *FileLedger) AppendOrgMerkleInfo(key []byte, orgMerkleInfo []byte) error {
	// Check for FileLedgerOrgStore implementation and append info
	os, exist := fl.blockStore.(FileLedgerOrgStore)
	if !exist {
		panic("AppendOrgMerkleInfo :: Method not implemented")
	}
	return os.AppendOrgMerkleInfo(key, orgMerkleInfo)
}

/*
DRUNIX: AppendOrgBlockIndex adds org-specific block index to the block store
*/
func (fl *FileLedger) AppendOrgBlockIndex(key []byte, orgVal []byte) error {
	// Check for FileLedgerOrgStore implementation and append index
	os, exist := fl.blockStore.(FileLedgerOrgStore)
	if !exist {
		panic("AppendOrgBlockIndex :: Method not implemented")
	}
	return os.AppendOrgBlockIndex(key, orgVal)
}

/*
DRUNIX: SaveChannelHeadInfo saves the channel head information to the block store
*/
func (fl *FileLedger) SaveChannelHeadInfo(key []byte, channelHeadInfo []byte) error {
	os, exist := fl.blockStore.(FileLedgerOrgStore)
	if !exist {
		panic("SaveChannelHeadInfo :: Method not implemented")
	}
	return os.SaveChannelHeadInfo(key, channelHeadInfo)
}

/*
DRUNIX: GetOrgMetaValue retrieves the organization-specific metadata value from the block store
*/
func (fl *FileLedger) GetOrgMetaValue(key []byte) ([]byte, error) {
	os, exist := fl.blockStore.(FileLedgerOrgStore)
	if !exist {
		panic("GetOrgMetaValue :: Method not implemented")
	}
	return os.GetOrgMetaValue(key)
}

/*
DRUNIX: GetSparseChannel returns the sparse channel for block retrieval
*/
func (fl *FileLedger) GetSparseChannel() (chan *cb.Block, error) {
	return fl.sparseCh, nil
}

func (fl *FileLedger) AppendSavePoint(key []byte, num []byte) error {
	os, exist := fl.blockStore.(FileLedgerOrgStore)
	if !exist {
		panic("AppendSavePoint :: Method not implemented")
	}
	return os.AppendSavePoint(key, num)
}

func (fl *FileLedger) GetSavePoint(key []byte) ([]byte, error) {
	os, exist := fl.blockStore.(FileLedgerOrgStore)
	if !exist {
		panic("GetSavePoint :: Method not implemented")
	}
	return os.GetSavePoint(key)
}
