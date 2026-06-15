/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/

package privdata


import (
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/npci/drunix/core/common/privdata"
	"github.com/npci/drunix/core/ledger"
	privdatacommon "github.com/npci/drunix/gossip/privdata/common"
	"github.com/npci/drunix/protoutil"
	"github.com/pkg/errors"
)

/*
DRUNIX:
	methods to fetch transient data for lite transactions
*/

type ltxInfo struct {
	channelID    string
	txID         string
	endorsements []*common.Endorsement
	txRWSet      *common.TxReadWriteSet
}

// getTxPvtdataInfoFromBlock parses the block transactions and returns the list of private data items in the block.
// Note that this peer's eligibility for the private data is not checked here.
func (c *coordinator) getLtxPvtdataInfoFromBlock(block *common.Block) ([]*ledger.LtxPvtdataInfo, error) {
	txPvtdataItemsFromBlock := []*ledger.LtxPvtdataInfo{}

	if block.Metadata == nil || len(block.Metadata.Metadata) <= int(common.BlockMetadataIndex_TRANSACTIONS_FILTER) {
		return nil, errors.New("Block.Metadata is nil or Block.Metadata lacks a Tx filter bitmap")
	}
	txsFilter := txValidationFlags(block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER])
	data := block.Data.Data
	if len(txsFilter) != len(block.Data.Data) {
		return nil, errors.Errorf("block data size(%d) is different from Tx filter size(%d)", len(block.Data.Data), len(txsFilter))
	}

	for seqInBlock, txEnvBytes := range data {
		invalid := txsFilter[seqInBlock] != uint8(peer.TxValidationCode_VALID)
		txInfo, err := getLtxInfoFromTransactionBytes(txEnvBytes)
		if err != nil {
			continue
		}

		colPvtdataInfo := []*ledger.LtxCollectionPvtdataInfo{}
		for _, ns := range txInfo.txRWSet.NsRwset {
			for _, hashedCollection := range ns.CollectionHashedRwset {
				// skip if no writes
				if !ltxContainsWrites(txInfo.txID, ns.Namespace, hashedCollection) {
					continue
				}
				cc := privdata.CollectionCriteria{
					Channel:    txInfo.channelID,
					Namespace:  ns.Namespace,
					Collection: hashedCollection.CollectionName,
				}

				colConfig, err := c.CollectionStore.RetrieveCollectionConfig(cc)
				if err != nil {
					c.logger.Warningf("Failed to retrieve collection config for collection criteria [%#v]: %s", cc, err)
					return nil, err
				}
				col := &ledger.LtxCollectionPvtdataInfo{
					Namespace:        ns.Namespace,
					Collection:       hashedCollection.CollectionName,
					ExpectedHash:     hashedCollection.PvtRwsetHash,
					CollectionConfig: colConfig,
					Endorsers:        txInfo.endorsements,
				}
				colPvtdataInfo = append(colPvtdataInfo, col)
			}
		}
		txPvtdataToRetrieve := &ledger.LtxPvtdataInfo{
			TxID:                  txInfo.txID,
			Invalid:               invalid,
			SeqInBlock:            uint64(seqInBlock),
			CollectionPvtdataInfo: colPvtdataInfo,
		}
		txPvtdataItemsFromBlock = append(txPvtdataItemsFromBlock, txPvtdataToRetrieve)
	}

	return txPvtdataItemsFromBlock, nil
}

// getTxInfoFromTransactionBytes parses a transaction and returns info required for private data retrieval
func getLtxInfoFromTransactionBytes(envBytes []byte) (*ltxInfo, error) {
	txInfo := &ltxInfo{}
	env, err := protoutil.GetEnvelopeFromBlock(envBytes)
	if err != nil {
		logger.Warningf("Invalid envelope: %s", err)
		return nil, err
	}

	leanenv := env.LeanEnv
	// DRUNIX : for cc approve and commit transactions leanEnv will be nil so convert the enelope to leanEnv
	if env.LeanEnv == nil {
		leanenv, err = protoutil.GetLeanEnvFromCommonEnv(env)
		if err != nil {
			return nil, err
		}
	}

	txInfo.channelID = leanenv.ChannelId
	txInfo.txID = leanenv.TxId

	if int32(env.Type) != int32(common.HeaderType_ENDORSER_TRANSACTION) {
		err := errors.New("header type is not an endorser transaction")
		logger.Debugf("Invalid transaction type: %s", err)
		return nil, err
	}

	txInfo.endorsements = leanenv.Meta.Endorsements
	txInfo.txRWSet = leanenv.Results

	return txInfo, nil
}

func ltxContainsWrites(txID string, namespace string, colHashedRWSet *common.CollectionHashedReadWriteSet) bool {
	if colHashedRWSet.HashedRwset == nil {
		logger.Warningf("HashedRwset of tx [%s], namespace [%s], collection [%s] is nil", txID, namespace, colHashedRWSet.CollectionName)
		return false
	}
	if len(colHashedRWSet.HashedRwset.HashedWrites) == 0 && len(colHashedRWSet.HashedRwset.MetadataWrites) == 0 {
		logger.Debugf("HashedRWSet of tx [%s], namespace [%s], collection [%s] doesn't contain writes", txID, namespace, colHashedRWSet.CollectionName)
		return false
	}
	return true
}

type ltxDig2sources map[privdatacommon.DigKey][]*common.Endorsement

func (d2s ltxDig2sources) keys() []privdatacommon.DigKey {
	res := make([]privdatacommon.DigKey, 0, len(d2s))
	for dig := range d2s {
		res = append(res, dig)
	}
	return res
}
