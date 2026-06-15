/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/

package ledger

import (
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/peer"
)

type LtxPvtdataInfo struct {
	TxID                  string
	Invalid               bool
	SeqInBlock            uint64
	CollectionPvtdataInfo []*LtxCollectionPvtdataInfo
}

type LtxCollectionPvtdataInfo struct {
	Namespace, Collection string
	ExpectedHash          []byte
	CollectionConfig      *peer.StaticCollectionConfig
	Endorsers             []*common.Endorsement
}
