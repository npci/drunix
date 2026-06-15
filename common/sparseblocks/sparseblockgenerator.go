/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
Modifications Copyright National Payments Corporation of India
*/

/*
DRUNIX
*/
package sparseblock

import (
	"encoding/binary"
	"fmt"
	"strings"
	"sync"

	cb "github.com/hyperledger/fabric-protos-go/common"
	ab "github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric-protos-go/peer"

	"github.com/npci/drunix/common/channelconfig"
	"github.com/npci/drunix/common/deliver"
	"github.com/npci/drunix/common/flogging"
	"github.com/npci/drunix/common/ledger/blockledger"
	"github.com/npci/drunix/common/util"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/statedb"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/statedb/ordererstatedb"

	proto "github.com/golang/protobuf/proto"
	"github.com/npci/drunix/orderer/common/multichannel"
	"github.com/npci/drunix/protoutil"
	// _ "github.com/wealdtech/go-merkletree/v2"
)

var channelHeaderContainer = make(map[string][]byte, 0)

var channelHeaderContainerCond = sync.NewCond(&sync.Mutex{})

var cHeaderContainerMutex = sync.RWMutex{}

type OrgBlockTxnIndex struct {
	orgBlock   *cb.Block
	txnIdxList []uint64
}

// runs as a routine for each channel
// processFatBlocks retrieves the FatBlock (mainBlock) from sparseChannel,
// processes it to generate orgBlocks, and stores the blockData.
func processFatBlocks(cOChain *ChannelOrgChains, orgBlockCacheSize uint64, mvccApplicable bool) {

	chain := cOChain.chainSupport
	sparseMetadataRWriter := getSparseMetadataReadWriter(chain) //levelDBhelper
	var nextTobeProcessed uint64
	isSparseChainStart := false

	// nextTobeProcessed := uint64(0)
	var chHeader ChannelHeadInfoProto
	chHeader, err := getChannelHeader(sparseMetadataRWriter, cOChain)
	if err != nil {
		logger.Errorf("Error while getting channel header: %v", err)
		return
	}

	// this will pick from block Iterator instead of channel
	// channelMetadataBytes, err := sparseMetadataRWriter.GetOrgMetaValue([]byte(fmt.Sprint(cOChain.channelID, ".ChannelHeadInfo")))
	// if err != nil {
	// 	panic("unable to read from db")
	// }
	// if channelMetadataBytes != nil {
	// 	err = proto.Unmarshal(channelMetadataBytes, &chHeader)
	// 	if err != nil {
	// 		logger.Errorf("will be processing zeroth block")
	// 		panic("Unable to unmarshal channel header value in sparse blocks metadata space")
	// 	}
	// 	// nextTobeProcessed = chHeader.LastFatBlockNumberProcessed + 1
	// }

	// fatIterator, _ := getFatIterator(cOChain.chainSupport, nextTobeProcessed)

	if len(chHeader.OrgHeadPostion) == 0 {
		fatIter, _ := GetLastProcessedBlk(chain)
		blk, _ := fatIter.Next()
		if blk != nil {
			nextTobeProcessed = blk.Header.Number + 1
			isSparseChainStart = true
		}
		fatIter.Close()
	} else {
		nextTobeProcessed = chHeader.LastFatBlockNumberProcessed + 1
	}

	sparseCh, err := sparseMetadataRWriter.GetSparseChannel()
	if err != nil {
		logger.Errorf("error getting sparseChannel :%v", err)
		return
	}

	for {
		var fatBlock *cb.Block
		if !isSparseChainStart {
			fatBlock = <-sparseCh
			// fatBlock, _ := fatIterator.Next()
			// TODO: sync should happen independent of this loop
			if fatBlock.Header.Number != 0 && fatBlock.Header.Number > chHeader.LastFatBlockNumberProcessed+1 {
				logger.Errorf("sync will be called now")
				err := syncOrderer(cOChain, fatBlock.Header.Number, chHeader.LastFatBlockNumberProcessed+1,
					sparseMetadataRWriter, &chHeader, orgBlockCacheSize, mvccApplicable)
				if err != nil {
					panic("error in sync of orderer!!!")
				}
			}
		} else {
			fatIterator, _ := getFatIterator(cOChain.chainSupport, nextTobeProcessed)
			fatBlock, _ = fatIterator.Next()
			fatIterator.Close()
			isSparseChainStart = false
		}

		orgBlockMap, fatBlockMerkleInfo, newChHeader := createOrgBlocks(fatBlock, chHeader, chain, cOChain.versionDB, mvccApplicable)
		logger.Debugf("newChHeader.LastFatBlockNumberProcessed: %v orgHeadPosition length : %v\n",
			newChHeader.LastFatBlockNumberProcessed, len(newChHeader.OrgHeadPostion))

		err := storeBlockData(fatBlock, cOChain, orgBlockMap, orgBlockCacheSize, sparseMetadataRWriter, fatBlockMerkleInfo, newChHeader, &chHeader)
		if err != nil {
			logger.Errorf("error while storing blockData :%v", err)
			return
		}
	}
}

// If LastFatBlockNumberProcessed significantly lags behind fatBlock.Header.Number,
// syncOrderer will be called to synchronize.
// syncOrderer - creates orgBlock and stores data for the lagged blockNum.
func syncOrderer(cOChain *ChannelOrgChains, latestFatBlockNumber uint64, nextBlockToBeProcessed uint64,
	sparseMetadataRWriter blockledger.SparseMetadataReadWriter, chHeader *ChannelHeadInfoProto, orgBlockCacheSize uint64, mvccApplicable bool) error {

	var i uint64
	newFatIterator, _ := getFatIterator(cOChain.chainSupport, nextBlockToBeProcessed)
	defer newFatIterator.Close()
	for i = 0; i < (latestFatBlockNumber - nextBlockToBeProcessed); i++ {
		fatBlock, _ := newFatIterator.Next()
		logger.Warnf("syncing for block number: %v\n", fatBlock.Header.Number)
		orgBlockMap, fatBlockMerkleInfo, newChHeader := createOrgBlocks(fatBlock, *chHeader, cOChain.chainSupport, cOChain.versionDB, mvccApplicable)

		err := storeBlockData(fatBlock, cOChain, orgBlockMap, orgBlockCacheSize, sparseMetadataRWriter, fatBlockMerkleInfo, newChHeader, chHeader)
		if err != nil {
			return err
		}
	}

	return nil
}

// storeBlockData stores the orgBlockBytes in their respective cacheStore by iterating through orgBlockMap
// It appends fatBlockMerkleInfoBytes into sparseMetadataRWriter and stores channelHeaderMetadataBytes
// into channelHeaderContainer
func storeBlockData(fatBlock *cb.Block, cOChain *ChannelOrgChains, orgBlockMap map[string]*cb.Block, orgBlockCacheSize uint64,
	sparseMetadataRWriter blockledger.SparseMetadataReadWriter, fatBlockMerkleInfo FatBlockMerkleInfoProto,
	newChHeader ChannelHeadInfoProto, oldChHeader *ChannelHeadInfoProto) error {
	for orgID, orgBlock := range orgBlockMap {
		orgBlockBytes, err := protoutil.Marshal(orgBlock)
		if err != nil {
			logger.Panicf("failed to marshal:%v", err)
		}

		_, ok := cOChain.orgChainMap[orgID]

		if !ok {
			cOChain.orgChainMap[orgID] = NewOrgChain(orgID, cOChain.channelID, cOChain.chainSupport, orgBlockCacheSize)
		}

		cacheStore := cOChain.orgChainMap[orgID].oCStore
		orgBlockNumberBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(orgBlockNumberBytes, orgBlock.Header.Number)
		cacheStore.store.SetBig(orgBlockNumberBytes, orgBlockBytes)
		cacheStore.mspId, cacheStore.channelId = orgID, cOChain.channelID
		fatBlockNumberBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(fatBlockNumberBytes, fatBlock.Header.Number)
		err = sparseMetadataRWriter.AppendOrgBlockIndex([]byte(fmt.Sprintf("%v.%v.%v", cOChain.channelID, orgID, orgBlock.Header.Number)), fatBlockNumberBytes)
		if err != nil {
			return err
		}
	}

	fatBlockMerkleInfoBytes, err := proto.Marshal(&fatBlockMerkleInfo)
	if err != nil {
		return err
	}

	channelHeaderMetadataBytes, err := proto.Marshal(&newChHeader)
	if err != nil {
		return err
	}
	err = sparseMetadataRWriter.AppendOrgMerkleInfo([]byte(fmt.Sprint(cOChain.channelID, ".", newChHeader.LastFatBlockNumberProcessed)), fatBlockMerkleInfoBytes)
	if err != nil {
		return err
	}

	/*
		DRUNIX
		Added sync.Cond to broadcast any changes on channelHeaderContainer
	*/

	cHeaderContainerMutex.Lock()
	channelHeaderContainerCond.L.Lock()
	channelHeaderContainer[fmt.Sprint(cOChain.channelID, ".ChannelHeadInfo")] = channelHeaderMetadataBytes
	channelHeaderContainerCond.Broadcast()
	channelHeaderContainerCond.L.Unlock()
	cHeaderContainerMutex.Unlock()

	err = sparseMetadataRWriter.SaveChannelHeadInfo([]byte(fmt.Sprint(cOChain.channelID, ".ChannelHeadInfo")), channelHeaderMetadataBytes)
	if err != nil {
		return err
	}
	*oldChHeader = newChHeader

	return nil
}

// getChannelHeader retrieves and unmarshals the ChannelHeadInfoProto for a given channel.
// returns errors if encountered, or returns an empty header if no metadata is found.
func getChannelHeader(sparseMetadataRWriter blockledger.SparseMetadataReadWriter, cOChain *ChannelOrgChains) (ChannelHeadInfoProto, error) {
	var chHeader ChannelHeadInfoProto
	channelMetadataBytes, err := sparseMetadataRWriter.GetOrgMetaValue(fmt.Append(nil, cOChain.channelID, ".ChannelHeadInfo"))
	if err != nil {
		logger.Errorf("error received: %v", err)
		return chHeader, fmt.Errorf("error received: %v", err)
	}
	if channelMetadataBytes == nil {
		logger.Warnf("will be processing zeroth block")
		return chHeader, nil
	}
	err = proto.Unmarshal(channelMetadataBytes, &chHeader)
	if err != nil {
		panic("Unable to unmarshal channel header value in sparse blocks metadata space")
	}

	return chHeader, nil
}

func GetLastProcessedBlk(chain deliver.Chain) (blockledger.Iterator, uint64) {
	stopNum := chain.Reader().Height() - 2
	seekS := &ab.SeekSpecified{Number: stopNum}
	SeekPositionS := ab.SeekPosition_Specified{Specified: seekS}
	sp := ab.SeekPosition{Type: &SeekPositionS}
	return chain.Reader().Iterator(&sp)
}

var txnSoFar int

// createOrgBlocks creates orgBlock by iterating over the main block based on the orgInvolved in each txnEnvelope
// This is done by checking the type of txnEnvelope:
// - txnEnv.Type == config : txn is given to all the orgs present in the orgList
// - txnEnv.Type == cb.HeaderType_ENDORSER_TRANSACTION: orgBlocks are created on the basis of orgs involved in the txn
func createOrgBlocks(blk *cb.Block, chHeader ChannelHeadInfoProto, chainSupport *multichannel.ChainSupport,
	versionDB ordererstatedb.OrdererDBHandler, mvccApplicable bool) (map[string]*cb.Block, FatBlockMerkleInfoProto, ChannelHeadInfoProto) {
	LitePeerChainId := channelconfig.LitePeerChainId
	orgsFromConfig := []string{chainSupport.ChannelID()}

	if len(chHeader.OrgHeadPostion) == 0 {
		if blk.Header.Number != 0 {
			// orgsFromConfig = append(orgsFromConfig, LitePeerChainId)
			// this should include all the chains here to start with the save-point
			newConfigOrgs := []string{
				0: chainSupport.ChannelID(),
				1: LitePeerChainId,
			}
			fatIter, _ := GetLastProcessedBlk(chainSupport)
			fatblock, _ := fatIter.Next()
			logger.Warnf("block: [%v] received from iterator", fatblock.Header.Number)
			fatIter.Close()

			savepoint := fatblock.Header.Number
			sparseMetaData := getSparseMetadataReadWriter(chainSupport)
			savepointBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(savepointBytes, savepoint)
			err := sparseMetaData.AppendSavePoint(fmt.Appendf(nil, "savepoint-%v", chainSupport.ChannelID()), savepointBytes)
			if err != nil {
				panic("unable to store savepoint")
			}

			dataHash := protoutil.BlockHeaderHash(fatblock.Header)
			// dataHash := fatblock.Header.DataHash
			// lastConfigMetadata, err := protoutil.UnmarshalMetadata(blk.Metadata.Metadata[cb.BlockMetadataIndex_LAST_CONFIG])
			// if err != nil {
			// 	logger.Errorf("error while unmarshalling BlockMetadataIndex_LAST_CONFIG")
			// }
			// lastConfigValue := &cb.LastConfig{}
			// err = proto.Unmarshal(lastConfigMetadata.Value, lastConfigValue)
			// if err != nil {
			// 	logger.Errorf("error while unmarshalling config value")
			// }
			var lastConfigValue uint64
			isConfig := protoutil.IsConfigBlock(fatblock)
			if !isConfig {
				lastConfigValue, err = protoutil.GetLastConfigIndexFromBlock(fatblock)
				if err != nil {
					logger.Errorf("error while unmarshalling config value")
				}
			} else {
				lastConfigValue = blk.Header.Number
			}

			logger.Warn("********-----------SWITCHING TO Sparse-Orderer------*********")

			chHeader = ChannelHeadInfoProto{
				LastFatBlockNumberProcessed: uint64(savepoint),
				OrgHeadPostion:              map[string]*LastBlockMappingProto{},
			}

			for _, org := range newConfigOrgs {
				chHeader.OrgHeadPostion[org] = &LastBlockMappingProto{
					LastOrgBlockNum:    savepoint,
					LastOrgBlockHash:   dataHash,
					SourceFatBlockNum:  savepoint,
					LastConfigBlockNum: lastConfigValue,
				}
			}

		} else {
			chHeader = ChannelHeadInfoProto{
				LastFatBlockNumberProcessed: 0,
				OrgHeadPostion:              map[string]*LastBlockMappingProto{},
			}
		}
	}

	// if len(chHeader.OrgHeadPostion) == 0 {
	// 	chHeader = ChannelHeadInfoProto{
	// 		LastFatBlockNumberProcessed: 0,
	// 		OrgHeadPostion:              map[string]*LastBlockMappingProto{},
	// 	}
	// }

	fatTxnDetailsMVCCMap := make(map[string]*statedb.VersionMVCC, 0)
	sparseTxnFilterMap := make(map[string]*SparseTxnFilterProto, 0)
	var orgTxnEnvelopeMap = make(map[string][]ordererstatedb.TxnDetails)
	var localOrgBlocks = make(map[string]*cb.Block)
	var fatBlockMerkleInfo = FatBlockMerkleInfoProto{
		MerkleRoot: []byte{},
		OrgHashMap: make(map[string]*OrgBlockDetailsProto),
	}

	configBlockCheck := protoutil.IsConfigBlock(blk)
	txnSoFar += len(blk.Data.Data)
	logger.Errorf("Txns in fatBlock:[%v] is : [%v] and txns so far: [%v]", blk.Header.Number, len(blk.Data.Data), txnSoFar)

	// defer func(txns int) {
	// 	calcTps.increment(1, txns)
	// 	calcTps.messageReceived <- true
	// }(len(blk.Data.Data))

	aggregateOrgEnvelope(blk, orgsFromConfig, mvccApplicable, versionDB, fatTxnDetailsMVCCMap, sparseTxnFilterMap, orgTxnEnvelopeMap)
	if mvccApplicable {
		done := applyBatchMVCC(fatTxnDetailsMVCCMap, versionDB)
		if !done {
			logger.Panicf("unable to write batch MVCC")
		}
	}
	clear(fatTxnDetailsMVCCMap)

	// var orgEnvBytes [][]byte
	txnIdListMap := make(map[string][]uint64, 0)

	for orgId, txnDetailsList := range orgTxnEnvelopeMap {
		var orgBlockNum uint64 = 0
		var prevOrgBlockHash []byte

		orgHeader, ok := chHeader.OrgHeadPostion[orgId]
		if ok {
			prevOrgBlockHash = orgHeader.LastOrgBlockHash
			orgBlockNum = orgHeader.LastOrgBlockNum + 1
		}
		orgBlock, txnIdxList := createNextBlock(txnDetailsList, prevOrgBlockHash, orgBlockNum)

		orgBlock.BlockType = blk.BlockType
		if configBlockCheck {
			orgBlock.BlockType = cb.HeaderType_CONFIG
		}

		txnIdListMap[orgId] = txnIdxList
		localOrgBlocks[orgId] = orgBlock
		// orgEnvBytes = append(orgEnvBytes, orgBlock.Header.DataHash)
	}

	// TODO: since we're not utilsing merkle proof we can remove it
	/*
		Drunix: since merkle proof is not required now
	*/
	// tree, err := merkletree.New(orgEnvBytes)
	// if err != nil {
	// 	panic(err)
	// }
	// root := tree.Root()
	chHeader.LastFatBlockNumberProcessed = blk.Header.Number
	// fatBlockMerkleInfo.MerkleRoot = root
	storeOrgBlockSkeleton(fatBlockMerkleInfo, localOrgBlocks, configBlockCheck, chainSupport, sparseTxnFilterMap, chHeader, txnIdListMap)

	return localOrgBlocks, fatBlockMerkleInfo, chHeader
}

func aggregateOrgEnvelope(blk *cb.Block, orgsFromConfig []string, mvccApplicable bool, versionDB ordererstatedb.OrdererDBHandler,
	fatTxnDetailsMVCCMap map[string]*statedb.VersionMVCC, sparseTxnFilterMap map[string]*SparseTxnFilterProto,
	orgTxnEnvelopeMap map[string][]ordererstatedb.TxnDetails) {

	configOrgList := make([]string, len(orgsFromConfig))
	copy(configOrgList, orgsFromConfig)
	configOrgList = append(configOrgList, channelconfig.LitePeerChainId)

	isApproveCommitBlockType, blockType := false, cb.HeaderType(0)
	for txnIndex, transactionEnvelopeBytes := range blk.Data.Data {
		var (
			txnEnv     *cb.Envelope
			txnChanHdr *cb.ChannelHeader
			payload    *cb.Payload
			err        error
			txType     cb.HeaderType
			txId       string
		)

		if txnEnv, err = protoutil.GetEnvelopeFromBlock(transactionEnvelopeBytes); err == nil {
			// if txnEnv.Type == cb.HeaderType_ENDORSER_TRANSACTION {
			// 	txType = cb.HeaderType_ENDORSER_TRANSACTION
			// }
			if txnEnv.LeanEnv != nil {
				txType = txnEnv.Type
				txId = txnEnv.LeanEnv.TxId
			} else {
				if payload, err = protoutil.UnmarshalPayload(txnEnv.Payload); err == nil {
					txnChanHdr, err = protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
					if err != nil {
						logger.Panicf("error while unmarshalling :%v", err)
					}
				}
				txType = cb.HeaderType(txnChanHdr.Type)
				if txnEnv.Type != cb.HeaderType_MESSAGE {
					txType = txnEnv.Type
				}
				txId = txnChanHdr.TxId
			}
		}

		var txnDetails = ordererstatedb.TxnDetails{
			Indx: uint64(txnIndex),
			// Env:         txnEnv,
			TxnEnvBytes: transactionEnvelopeBytes,
		}

		fatBlockNum := blk.Header.Number
		txnIdx := txnDetails.Indx
		orgList := make([]string, len(orgsFromConfig)+1)
		var txRWSet *cb.TxReadWriteSet
		// TODO: to be handled in a better way for approve chaincode txn
		if txType == cb.HeaderType_CONFIG {
			for _, org := range configOrgList {
				sparseFilter, ok := sparseTxnFilterMap[org]
				if !ok {
					sparseFilter = &SparseTxnFilterProto{
						FatBlockNumber: blk.Header.Number,
						TxnInfoList:    []*TxnInfoProto{},
					}
					sparseTxnFilterMap[org] = sparseFilter
				}
				sparseFilter.TxnInfoList = append(sparseFilter.TxnInfoList, &TxnInfoProto{
					IndexInFatBlock: txnDetails.Indx,
					TxnMVCCStatus:   int32(txnDetails.TxnMVCCStatus),
				})

				orgTxnEnvelopeMap[org] = append(orgTxnEnvelopeMap[org], txnDetails)
			}

			logger.Warn("config transaction received for chain ")
			continue
		} else if txType == cb.HeaderType_ENDORSER_TRANSACTION ||
			txType == cb.HeaderType_APPROVE || txType == cb.HeaderType_COMMIT {
			//DRUNIX: if payload is not nil for an endorser transaction then it means that this is an approve or commit cc transaction.
			/*
				DRUNIX
				We will add approve/commit blocks to lpeer chain, we will check whether
				payload is empty or not, since payload will not be empty since cli is a vanilla flow
				commented code for reference
				// approve-commit flow needs to be handled before adding to the raft,
				// since updating here will change the original hash of the block
			*/
			if txnEnv.LeanEnv == nil {
				logger.Warnf("txnEnv.LeanEnv is nil it could be vanilla-format txn")
				leanEnv, err := protoutil.GetLeanEnvFromCommonEnv(txnEnv)
				if err != nil {
					logger.Panicf("error while unmarshalling :%v", err)
				}
				txRWSet = leanEnv.Results
				// txnEnv.Type = cb.HeaderType_ENDORSER_TRANSACTION
				// txnDetails.Env = txnEnv
				// txnDetails.TxnEnvBytes = protoutil.MarshalOrPanic(txnEnv)
			} else {
				txRWSet = txnEnv.LeanEnv.Results
			}
		} else {
			logger.Infof("Unknown transaction type [%s] in block number [%d] transaction index [%s]\n",
				txType, blk.Header.Number, txId)
		}

		if txRWSet != nil {
			// If we want the split based on chaincode logic
			// for _, nsRwSet := range txRWSet.NsRwSets {

			// 	kvRwSet := nsRwSet.KvRwSet
			// 	var OrgsString string
			// 	for _, writekv := range kvRwSet.Writes {
			// 		if writekv.Key == "_ORGS_INVOLVED" {
			// 			OrgsString = string(writekv.Value)
			// 			break
			// 		}
			// 	}
			// 	orgList = parseOrgs(OrgsString, orgsFromConfig)
			// }
			// fetching the orgsInvolved in that txn (blocks will be distributed accordingly)
			if txnChanHdr != nil {
				orgList = txnChanHdr.GetOrgsInvolved()
			} else {
				orgList = txnEnv.LeanEnv.OrgsInvolved
			}
			if len(orgList) == 0 {
				orgList = parseOrgs("", orgsFromConfig)
			}

			if mvccApplicable {
				if txnFilteringStatus := validateEndorserTxn(txRWSet, fatTxnDetailsMVCCMap, versionDB, &txnDetails, fatBlockNum, txnIdx); !txnFilteringStatus {
					txnDetails.TxnMVCCStatus = peer.TxValidationCode_MVCC_READ_CONFLICT
				}
			}
			if txType == cb.HeaderType_APPROVE || txType == cb.HeaderType_COMMIT {
				isApproveCommitBlockType, blockType = true, txType
				orgList = configOrgList
			}
			for _, org := range orgList {
				sparseFilter, ok := sparseTxnFilterMap[org]
				if !ok {
					sparseFilter = &SparseTxnFilterProto{
						FatBlockNumber: blk.Header.Number,
						TxnInfoList:    []*TxnInfoProto{},
					}
					sparseTxnFilterMap[org] = sparseFilter
				}
				sparseFilter.TxnInfoList = append(sparseFilter.TxnInfoList, &TxnInfoProto{
					IndexInFatBlock: txnDetails.Indx,
					TxnMVCCStatus:   int32(txnDetails.TxnMVCCStatus),
				})

				orgTxnEnvelopeMap[org] = append(orgTxnEnvelopeMap[org], txnDetails)
			}
		}
	}
	if isApproveCommitBlockType {
		blk.BlockType = blockType
	}
}

func validateEndorserTxn(txRWSet *cb.TxReadWriteSet, fatTxnDetailsMVCCMap map[string]*statedb.VersionMVCC,
	versionDB ordererstatedb.OrdererDBHandler, txnDetails *ordererstatedb.TxnDetails, fatBlockNum, txnIdx uint64) bool {
	txnFilteringStatus := true
	for _, nsRwSet := range txRWSet.NsRwset {
		txnFilteringStatus = validateNameSpaceReadSet(fatTxnDetailsMVCCMap, nsRwSet, versionDB, txnDetails)
		if !txnFilteringStatus {
			break
		}
	}

	if txnFilteringStatus {
		for _, nsRwSet := range txRWSet.NsRwset {
			if !applyNameSpaceWriteSet(fatTxnDetailsMVCCMap, versionDB, fatBlockNum, txnIdx, nsRwSet) {
				txnFilteringStatus = false
			}
		}
	}

	return txnFilteringStatus
}

/*
DRUNIX: This function is used to store org block skeleton per org
*/
func storeOrgBlockSkeleton(fatBlockMerkleInfo FatBlockMerkleInfoProto, localOrgBlocks map[string]*cb.Block, configBlockCheck bool,
	chainSupport *multichannel.ChainSupport, sparseTxnFilterMap map[string]*SparseTxnFilterProto, chHeader ChannelHeadInfoProto, txnIdListMap map[string][]uint64) {
	for orgId, orgBlock := range localOrgBlocks {

		/*

			Drunix: since merkle proof is not required now
			proof, err := tree.GenerateProof(orgBlock.Header.DataHash, 0)
			if err != nil {
				panic(" error in creating GenerateProof")
			}
			marshalledProof, er := json.Marshal(proof)
			if er != nil {
				panic("error in marshalling proof")
			}
		*/
		sparseFilterBytes, err := proto.Marshal(sparseTxnFilterMap[orgId])
		if err != nil {
			logger.Errorf("error while marshalling sparseFilter :%v", err)
		}

		var lastConfigBlockNumber uint64
		if configBlockCheck {
			lastConfigBlockNumber = orgBlock.Header.Number
		} else {
			lastConfigBlockNumber = chHeader.OrgHeadPostion[orgId].LastConfigBlockNum
		}
		chHeader.OrgHeadPostion[orgId] = &LastBlockMappingProto{
			LastOrgBlockNum:    orgBlock.Header.Number,
			LastOrgBlockHash:   protoutil.BlockHeaderHash(orgBlock.Header),
			SourceFatBlockNum:  chHeader.LastFatBlockNumberProcessed,
			LastConfigBlockNum: lastConfigBlockNumber,
		}

		fatBlockMerkleInfo.OrgHashMap[orgId] = &OrgBlockDetailsProto{
			OrgBlockNumber: orgBlock.Header.Number,
			// Authpath:       marshalledProof,
			// Hash:               protoutil.BlockHeaderHash(orgBlock.Header),
			Hash:               orgBlock.Header.DataHash,
			TxnIndexList:       txnIdListMap[orgId],
			PrevioushHash:      orgBlock.Header.PreviousHash,
			PrevFatblockNumber: chHeader.OrgHeadPostion[orgId].SourceFatBlockNum,
			LastConfigBlockNum: lastConfigBlockNumber,
			SparseBlockFilter:  sparseFilterBytes,
		}

		addMetadataFields(fatBlockMerkleInfo, orgBlock, orgId, chainSupport)
	}
}

type BlockCreator struct {
	hash   []byte
	number uint64
	logger *flogging.FabricLogger
}

var blockCreators = map[string]*BlockCreator{}

// retrieveTxnListForOrgBlock accumulates envelopes in enevelopeList by iterating over the block
func retrieveTxnListForOrgBlock(blk *cb.Block, txnIdxList []uint64) [][]byte {
	envelopeList := make([][]byte, 0)
	for _, txnIdx := range txnIdxList {
		envelopeList = append(envelopeList, blk.Data.Data[txnIdx])
	}
	return envelopeList
}

// to get OrgBlock using fatBlockMerkleInfo and fatBlockNumber
// If orgBlock is not found in the cache, it needs to be assembled by assembleOrgBlock
// This is done by retrieving txnEnvelopeByteList using orgBlockDetails and fatblock and rest,
// and return the block by restructuring it again
func assembleOrgBlock(fatblockMerkleInfo FatBlockMerkleInfoProto, orgId string, fatBlockNum uint64,
	fatBlockChainReader blockledger.Reader) *cb.Block {

	logger.Warn("since the block is out of cache we need to assemble it")
	orgBlockDetails := fatblockMerkleInfo.OrgHashMap[orgId]
	fatBlock, err := fatBlockChainReader.RetrieveBlockByNumber(fatBlockNum)
	if err != nil {
		logger.Panicf("error while retreiving block by number :%v", err)
	}
	txnEnvelopeByteList := retrieveTxnListForOrgBlock(fatBlock, orgBlockDetails.TxnIndexList)
	data := &cb.BlockData{}

	data.Data = txnEnvelopeByteList
	orgBlock := protoutil.NewBlock(orgBlockDetails.OrgBlockNumber, orgBlockDetails.PrevioushHash)
	orgBlock.Header.DataHash = orgBlockDetails.Hash
	orgBlock.Data = data
	logger.Debugf("assemble org block called for Block Number: %v for org: %v", orgBlock.Header.Number, orgId)
	orgBlock.Metadata.Metadata[cb.BlockMetadataIndex_LAST_CONFIG] = fatblockMerkleInfo.OrgHashMap[orgId].MetadataLastConfig
	orgBlock.Metadata.Metadata[cb.BlockMetadataIndex_SIGNATURES] = fatblockMerkleInfo.OrgHashMap[orgId].MetadataSignature

	/*
		DRUNIX: uncomment if merkle prrof is required
	*/
	// orgBlock.Metadata.Metadata[cb.BlockMetadataIndex_MERKLE_ROOT] = fatblockMerkleInfo.MerkleRoot
	// orgBlock.Metadata.Metadata[cb.BlockMetadataIndex_ORG_BLOCK_PROOF] = fatblockMerkleInfo.OrgHashMap[orgId].Authpath
	orgBlock.Metadata.Metadata[cb.BlockMetadataIndex_SPARSE_TXN_FILTER] = fatblockMerkleInfo.OrgHashMap[orgId].SparseBlockFilter

	return orgBlock
}

// createNextBlock creates next orgBlock from txnsList and previous hash by calculating the data and dataHash
func createNextBlock(txnDetailsList []ordererstatedb.TxnDetails, previousHash []byte, orgBlockNumber uint64) (*cb.Block, []uint64) {

	data := &cb.BlockData{
		Data: make([][]byte, len(txnDetailsList)),
	}

	txnIdxList := make([]uint64, len(txnDetailsList))
	for i, txnDetails := range txnDetailsList {

		// txnEnvBytes, err := proto.Marshal(txnDetails.Env)
		// if err != nil {
		// 	logger.Panicf("error while marshalling envelope :%v", err)
		// }
		data.Data[i] = txnDetails.TxnEnvBytes
		txnIdxList[i] = txnDetails.Indx
	}

	block := protoutil.NewBlock(orgBlockNumber, previousHash)
	block.Header.DataHash = protoutil.BlockDataHash(data)
	block.Data = data

	return block, txnIdxList
}

// parseOrgs returns a slice of orgs from the input string or orgsFromConfig if the input is empty
func parseOrgs(orgs string, orgsFromConfig []string) []string {
	if orgs == "" {
		return orgsFromConfig
	}
	orgs = strings.Trim(orgs, "\"")
	return strings.Split(orgs, ",")
}

// Adds metadata fields to orgBlock, excluding Merkle proof as it's no longer required
func addMetadataFields(fatblockMerkleInfo FatBlockMerkleInfoProto, orgBlock *cb.Block, orgID string, chainSupport *multichannel.ChainSupport) {
	/*
		Drunix: since merkle proof is not required now so removing from metadata
	*/
	// orgBlock.Metadata.Metadata[cb.BlockMetadataIndex_ORG_BLOCK_PROOF] = protoutil.MarshalOrPanic(&cb.Metadata{Value: fatblockMerkleInfo.OrgHashMap[orgID].Authpath})
	// orgBlock.Metadata.Metadata[cb.BlockMetadataIndex_MERKLE_ROOT] = protoutil.MarshalOrPanic(&cb.Metadata{Value: fatblockMerkleInfo.MerkleRoot})
	orgBlock.Metadata.Metadata[cb.BlockMetadataIndex_SPARSE_TXN_FILTER] = protoutil.MarshalOrPanic(&cb.Metadata{Value: fatblockMerkleInfo.OrgHashMap[orgID].SparseBlockFilter})
	// orgBlock.Metadata.Metadata[cb.BlockMetadataIndex_LAST_CONFIG] = fatBlock.Metadata.Metadata[cb.BlockMetadataIndex_LAST_CONFIG]
	addOrgLastConfig(orgBlock, fatblockMerkleInfo.OrgHashMap[orgID])
	fatBlockConsenterMetadata, err := protoutil.GetConsenterMetadataFromBlock(orgBlock)
	if err != nil {
		logger.Panicf("Got error while fetching block metadata: %v", err)
	}
	addOrgBlockSignature(orgBlock, fatBlockConsenterMetadata.Value, fatblockMerkleInfo.OrgHashMap[orgID], chainSupport)

}

// addOrgLastConfig updates the lastConfig for orgBlock
func addOrgLastConfig(orgBlock *cb.Block, orgBlockDetails *OrgBlockDetailsProto) {
	lastConfigValue := protoutil.MarshalOrPanic(&cb.LastConfig{Index: orgBlockDetails.LastConfigBlockNum})
	orgBlock.Metadata.Metadata[cb.BlockMetadataIndex_LAST_CONFIG] = protoutil.MarshalOrPanic(&cb.Metadata{
		Value: lastConfigValue,
	})
	orgBlockDetails.MetadataLastConfig = orgBlock.Metadata.Metadata[cb.BlockMetadataIndex_LAST_CONFIG]
}

func addOrgBlockSignature(orgBlock *cb.Block, consenterMetadata []byte, orgBlockDetails *OrgBlockDetailsProto, chainSupport *multichannel.ChainSupport) {

	blockSignature := &cb.MetadataSignature{
		SignatureHeader: protoutil.MarshalOrPanic(protoutil.NewSignatureHeaderOrPanic(chainSupport)),
	}
	blockSignatureValue := protoutil.MarshalOrPanic(&cb.OrdererBlockMetadata{
		LastConfig:        &cb.LastConfig{Index: orgBlockDetails.LastConfigBlockNum},
		ConsenterMetadata: protoutil.MarshalOrPanic(&cb.Metadata{Value: consenterMetadata}),
	})
	blockSignature.Signature = protoutil.SignOrPanic(
		chainSupport,
		util.ConcatenateBytes(blockSignatureValue, blockSignature.SignatureHeader, protoutil.BlockHeaderBytes(orgBlock.Header)),
	)
	orgBlock.Metadata.Metadata[cb.BlockMetadataIndex_SIGNATURES] = protoutil.MarshalOrPanic(&cb.Metadata{
		Value: blockSignatureValue,
		Signatures: []*cb.MetadataSignature{
			blockSignature,
		},
	})
	orgBlockDetails.MetadataSignature = orgBlock.Metadata.Metadata[cb.BlockMetadataIndex_SIGNATURES]

}

func validateNameSpaceReadSet(fatTxnDetailsMVCCMap map[string]*statedb.VersionMVCC, nsRwSet *cb.NsReadWriteSet, ordererDBHandler ordererstatedb.OrdererDBHandler, txnDetails *ordererstatedb.TxnDetails) bool {
	var readSetsValid bool

	logger.Debugf("Validating for namespace: %v", nsRwSet.Namespace)
	isReadSetValid, err := ordererDBHandler.ValidateReadSet(fatTxnDetailsMVCCMap, nsRwSet, txnDetails)
	if isReadSetValid && err == nil {
		isNsHashedReadSetsValid, err := ordererDBHandler.ValidateNsHashedReadSets(fatTxnDetailsMVCCMap, nsRwSet, txnDetails)
		if isNsHashedReadSetsValid && err == nil {
			readSetsValid = true
		}
	}
	return readSetsValid
}

func applyNameSpaceWriteSet(fatTxnDetailsMVCCMap map[string]*statedb.VersionMVCC, ordererDBHandler ordererstatedb.OrdererDBHandler,
	fatBlockNum uint64, txnIdx uint64, nsRwSet *cb.NsReadWriteSet) bool {

	logger.Debugf("Apply for namespace: %v", nsRwSet.Namespace)
	version := rwsetutil.NewKeyVersion(fatBlockNum, txnIdx)
	isApplied, err := ordererDBHandler.ApplyWriteSet(fatTxnDetailsMVCCMap, nsRwSet.Namespace, nsRwSet.Rwset.Writes, version)
	if !isApplied || err != nil {
		logger.Errorf("Txn failed for Apply WriteSet :%v", err)
		return false
	}

	isApplied, err = ordererDBHandler.ApplyWriteHashSet(fatTxnDetailsMVCCMap, nsRwSet.Namespace, nsRwSet.CollectionHashedRwset, version)
	if !isApplied || err != nil {
		logger.Errorf("Txn failed for Apply WriteHashSet :%v", err)
		return false
	}

	return true
}

func applyBatchMVCC(fatTxnDetailsMVCCMap map[string]*statedb.VersionMVCC, ordererDBHandler ordererstatedb.OrdererDBHandler) bool {
	isApplied, err := ordererDBHandler.ApplyBatch(fatTxnDetailsMVCCMap)
	if !isApplied || err != nil {
		logger.Errorf("Txn failed for Apply WriteHashSet :%v", err)
		return false
	}

	return true
}

// var calcTps CalcTps

// type CalcTps struct {
// 	txnCounter      atomic.Uint64
// 	blkCounter      atomic.Uint64
// 	startTime       time.Time
// 	endTime         time.Time
// 	messageReceived chan bool
// }

// func (c *CalcTps) increment(blkCount int, txnCount int) {
// 	c.blkCounter.Add(uint64(blkCount))
// 	c.txnCounter.Add(uint64(txnCount))
// }

// func init() {

// 	calcTps = CalcTps{
// 		txnCounter:      atomic.Uint64{},
// 		blkCounter:      atomic.Uint64{},
// 		startTime:       time.Time{},
// 		endTime:         time.Time{},
// 		messageReceived: make(chan bool),
// 	}

// 	go func() {
// 		for {
// 			select {
// 			case <-calcTps.messageReceived:
// 				if calcTps.startTime.IsZero() {
// 					calcTps.startTime = time.Now()
// 				}
// 				calcTps.endTime = time.Now()
// 			case <-time.After(30 * time.Second):
// 				if !calcTps.startTime.IsZero() {
// 					duration := calcTps.endTime.Sub(calcTps.startTime)
// 					fmt.Println("Start Time    : ", calcTps.startTime)
// 					fmt.Println("End Time      : ", calcTps.endTime)
// 					fmt.Println("Txn Count     : ", calcTps.txnCounter.Load())
// 					fmt.Println("Blk Count     : ", calcTps.blkCounter.Load())
// 					fmt.Println("Time Taken    : ", duration)
// 					fmt.Println("TPS           : ", float64(calcTps.txnCounter.Load())/float64(duration.Seconds()))
// 					fmt.Println("Blks/Sec      : ", float64(calcTps.blkCounter.Load())/float64(duration.Seconds()))
// 					calcTps.startTime = time.Time{}
// 					calcTps.txnCounter.Store(0)
// 					calcTps.blkCounter.Store(0)
// 				}
// 			}
// 		}
// 	}()
// }
