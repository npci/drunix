/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/

package validation

import (
	"bytes"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	sparseblock "github.com/npci/drunix/common/sparseblocks"
	"github.com/npci/drunix/core/ledger"
	"github.com/npci/drunix/core/ledger/internal/version"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/npci/drunix/core/ledger/util"
	"github.com/npci/drunix/internal/pkg/txflags"
	"github.com/npci/drunix/protoutil"
	"github.com/pkg/errors"
)

type ltxBlock struct {
	num uint64
	txs []*ltxTransaction

	// sparseblockchain:Filter for failed txns seq number
	sparseblockchainFilter sparseblock.SparseTxnFilterProto
	// sparseFilterExists
	sparseFilterExists bool
}

type ltxTransaction struct {
	indexInBlock            int
	id                      string
	rwset                   *common.TxReadWriteSet
	validationCode          peer.TxValidationCode
	containsPostOrderWrites bool
	channelId               string
}

type LtxStatInfo struct {
	TxIDFromChannelHeader string
	ValidationCode        peer.TxValidationCode
	TxType                common.HeaderType
	ChaincodeID           *common.ChaincodeID
	ChaincodeEventData    []byte
	NumCollections        int
}

func (p *CommitBatchPreparer) ValidateAndPrepareBatchLtx(blockAndPvtdata *ledger.BlockAndPvtData,
	doMVCCValidation bool) (*privacyenabledstate.UpdateBatch, []*AppInitiatedPurgeUpdate, []*LtxStatInfo, error) {
	blk := blockAndPvtdata.Block
	logger.Debugf("ValidateAndPrepareBatch() for block number = [%d]", blk.Header.Number)
	var internalBlock *ltxBlock
	var txsStatInfo []*LtxStatInfo
	var pubAndHashUpdates *publicAndHashUpdates
	var pvtUpdates *privacyenabledstate.PvtUpdateBatch
	var purgeUpdates []*AppInitiatedPurgeUpdate
	var err error

	logger.Debug("preprocessing ProtoBlock...")
	if internalBlock, txsStatInfo, err = preprocessLtxProtoBlock(
		p.postOrderSimulatorProvider,
		p.db.ValidateKeyValue,
		blk,
		doMVCCValidation,
		p.customTxProcessors,
	); err != nil {
		return nil, nil, nil, err
	}

	if pubAndHashUpdates, purgeUpdates, err = p.validator.validateAndPrepareLtxBatch(internalBlock, doMVCCValidation); err != nil {
		return nil, nil, nil, err
	}
	logger.Debug("validating rwset...")
	if pvtUpdates, err = validateAndPrepareLtxPvtBatch(
		internalBlock,
		p.db,
		pubAndHashUpdates,
		blockAndPvtdata.PvtData,
	); err != nil {
		return nil, nil, nil, err
	}
	logger.Debug("postprocessing ProtoBlock...")
	postprocessLtxProtoBlock(blk, internalBlock)
	logger.Debug("ValidateAndPrepareBatch() complete")

	txsFilter := txflags.ValidationFlags(blk.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER])
	for i := range txsFilter {
		txsStatInfo[i].ValidationCode = txsFilter.Flag(i)
	}
	return &privacyenabledstate.UpdateBatch{
		PubUpdates:  pubAndHashUpdates.publicUpdates,
		HashUpdates: pubAndHashUpdates.hashUpdates,
		PvtUpdates:  pvtUpdates,
	}, purgeUpdates, txsStatInfo, nil
}

func validateAndPrepareLtxPvtBatch(
	blk *ltxBlock,
	db *privacyenabledstate.DB,
	pubAndHashUpdates *publicAndHashUpdates,
	pvtdata map[uint64]*ledger.TxPvtData,
) (*privacyenabledstate.PvtUpdateBatch, error) {
	pvtUpdates := privacyenabledstate.NewPvtUpdateBatch()
	metadataUpdates := metadataUpdates{}
	for _, tx := range blk.txs {
		if tx.validationCode != peer.TxValidationCode_VALID {
			continue
		}
		if !tx.containsPvtWrites() {
			continue
		}
		txPvtdata := pvtdata[uint64(tx.indexInBlock)]
		if txPvtdata == nil {
			continue
		}
		if requiresPvtdataValidation(txPvtdata) {
			if err := validateLtxPvtdata(tx, txPvtdata); err != nil {
				return nil, err
			}
		}
		var pvtRWSet *rwsetutil.TxPvtRwSet
		var err error
		if pvtRWSet, err = rwsetutil.TxPvtRwSetFromProtoMsg(txPvtdata.WriteSet); err != nil {
			return nil, err
		}
		addPvtRWSetToPvtUpdateBatch(pvtRWSet, pvtUpdates, version.NewHeight(blk.num, uint64(tx.indexInBlock)))
		addEntriesToMetadataUpdates(metadataUpdates, pvtRWSet)
	}
	if err := incrementPvtdataVersionIfNeeded(metadataUpdates, pvtUpdates, pubAndHashUpdates, db); err != nil {
		return nil, err
	}
	return pvtUpdates, nil
}

func validateLtxPvtdata(tx *ltxTransaction, pvtdata *ledger.TxPvtData) error {
	if pvtdata.WriteSet == nil {
		return nil
	}

	for _, nsPvtdata := range pvtdata.WriteSet.NsPvtRwset {
		for _, collPvtdata := range nsPvtdata.CollectionPvtRwset {
			collPvtdataHash := util.ComputeHash(collPvtdata.Rwset)
			hashInPubdata := tx.retrieveHash(nsPvtdata.Namespace, collPvtdata.CollectionName)
			if !bytes.Equal(collPvtdataHash, hashInPubdata) {
				return errors.Errorf(`hash of pvt data for collection [%s:%s] does not match with the corresponding hash in the public data. public hash = [%#v], pvt data hash = [%#v]`,
					nsPvtdata.Namespace, collPvtdata.CollectionName, hashInPubdata, collPvtdataHash)
			}
		}
	}
	return nil
}

func extractSparseFilter(blk *common.Block) (sparseblock.SparseTxnFilterProto, error) {

	if blk.Header.Number == 0 {
		logger.Warnf("Genesis Block received")
		return sparseblock.SparseTxnFilterProto{
			FatBlockNumber: 0,
			TxnInfoList:    []*sparseblock.TxnInfoProto{},
		}, nil
	}
	if len(blk.Metadata.Metadata) <= int(common.BlockMetadataIndex_SPARSE_TXN_FILTER) {
		return sparseblock.SparseTxnFilterProto{
			FatBlockNumber: 0,
			TxnInfoList:    []*sparseblock.TxnInfoProto{},
		}, fmt.Errorf("sparseTxnfilter not present in metdata")
	}
	sparseTxnFilterProto := blk.Metadata.Metadata[common.BlockMetadataIndex_SPARSE_TXN_FILTER]
	sparseTxnFilterProtoMetadata, err := protoutil.UnmarshalMetadata(sparseTxnFilterProto)
	if err != nil {
		logger.Errorf("error while unmarshalling metdata from block:", err)
		return sparseblock.SparseTxnFilterProto{}, err
	}

	var sparseTxnFilter sparseblock.SparseTxnFilterProto
	err = proto.Unmarshal(sparseTxnFilterProtoMetadata.Value, &sparseTxnFilter)
	if err != nil {
		return sparseblock.SparseTxnFilterProto{}, err
	}

	if len(sparseTxnFilter.TxnInfoList) == 0 {
		return sparseblock.SparseTxnFilterProto{}, fmt.Errorf("empty txnInfo")
	}

	return sparseTxnFilter, nil
}

func preprocessLtxProtoBlock(postOrderSimulatorProvider PostOrderSimulatorProvider,
	validateKVFunc func(key string, value []byte) error,
	blk *common.Block, doMVCCValidation bool,
	customTxProcessors map[common.HeaderType]ledger.CustomTxProcessor,
) (*ltxBlock, []*LtxStatInfo, error) {

	sparseFilterExist := true
	sparseFilter, err := extractSparseFilter(blk)
	if err != nil {
		logger.Infof("error received while receiving sparseFilter: %v", err)
		sparseFilterExist = false
	}

	b := &ltxBlock{
		num:                    blk.Header.Number,
		txs:                    make([]*ltxTransaction, len(blk.Data.Data)),
		sparseblockchainFilter: sparseFilter,
		sparseFilterExists:     sparseFilterExist,
	}

	txsStatInfo := []*LtxStatInfo{}
	// Committer validator has already set validation flags based on well formed tran checks
	txsFilter := txflags.ValidationFlags(blk.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER])
	for txIndex, envBytes := range blk.Data.Data {
		// DRUNIX-LTF-TODO : we can make this concurrent
		var env *common.Envelope
		var txid, channelid string
		var txType common.HeaderType
		var err error
		var leanenv *common.LeanEnvelope
		txStatInfo := &LtxStatInfo{TxType: -1}
		txsStatInfo = append(txsStatInfo, txStatInfo)
		if env, err = protoutil.GetEnvelopeFromBlock(envBytes); err == nil {
			leanenv = env.LeanEnv
			// DRUNIX: for cc approve and commit  `env.LeanEnv` will be nil so convert it to lite env
			if env.LeanEnv == nil {
				leanenv, err = protoutil.GetLeanEnvFromCommonEnv(env)
			}
			txid = leanenv.TxId
			channelid = leanenv.ChannelId
			txType = env.Type
		}
		txStatInfo.TxIDFromChannelHeader = txid

		/*
			DRUNIX
			Since fat block index is maintained in an array, we are appending both valid and invalid txns to the blockAdapter, so that indices are not spoiled.
			And later in validateAndPrepareBatch the invalid txns are removed.
			Whereas in vanila only valid txns are appended.
		*/

		// if txsFilter.IsInvalid(txIndex) {
		// 	// Skipping invalid transaction
		// 	logger.Warningf("Channel [%s]: Block [%d] Transaction index [%d] TxId [%s]"+
		// 		" marked as invalid by committer. Reason code [%s]",
		// 		channelid, blk.Header.Number, txIndex, txid,
		// 		txsFilter.Flag(txIndex).String())
		// 	continue
		// }
		if err != nil {
			return nil, nil, err
		}

		var txRWSet *common.TxReadWriteSet
		var containsPostOrderWrites bool

		logger.Debugf("txType=%s", txType)
		txStatInfo.TxType = txType

		// extract actions from the envelope message
		txStatInfo.ChaincodeID = leanenv.Meta.ChaincodeID
		txRWSet = leanenv.Results
		txnValidationCode := txsFilter.Flag(txIndex)

		if txRWSet != nil {
			txStatInfo.NumCollections = len(txRWSet.NsRwset)
			/*
				commented the below code because `validateKVFunc` method has no body it's just returning nil.
			*/
			/*
				if err := validateWriteset(txRWSet, validateKVFunc); err != nil {
					logger.Warningf("Channel [%s]: Block [%d] Transaction index [%d] TxId [%s]"+
						" marked as invalid. Reason code [%s]",
						chdr.GetChannelId(), blk.Header.Number, txIndex, chdr.GetTxId(), peer.TxValidationCode_INVALID_WRITESET)
					txsFilter.SetFlag(txIndex, peer.TxValidationCode_INVALID_WRITESET)
					continue
				}
			*/
			b.txs[txIndex] = &ltxTransaction{
				indexInBlock:            txIndex,
				id:                      txid,
				rwset:                   txRWSet,
				containsPostOrderWrites: containsPostOrderWrites,
				validationCode:          txnValidationCode,
				channelId:               channelid,
			}
		}
	}
	return b, txsStatInfo, nil
}

func postprocessLtxProtoBlock(blk *common.Block, validatedBlock *ltxBlock) {
	txsFilter := txflags.ValidationFlags(blk.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER])
	for _, tx := range validatedBlock.txs {
		txsFilter.SetFlag(tx.indexInBlock, tx.validationCode)
	}
	blk.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER] = txsFilter
}
