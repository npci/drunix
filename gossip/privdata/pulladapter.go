/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/


package privdata

import (
	"bytes"

	"github.com/golang/protobuf/proto"
	"github.com/npci/drunix/gossip/api"
	"github.com/npci/drunix/gossip/common"
	privdatacommon "github.com/npci/drunix/gossip/privdata/common"
	"github.com/pkg/errors"
)

/*
DRUNIX:
	methods to fetch transient data for lite transactions
*/

func (p *puller) ltxFetch(dig2src ltxDig2sources) (*privdatacommon.FetchedPvtDataContainer, error) {
	// computeFilters returns a map from a digest to a routing filter
	dig2Filter, err := p.computeLtxFilters(dig2src)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return p.fetchPrivateData(dig2Filter)
}

func (p *puller) computeLtxFilters(dig2src ltxDig2sources) (digestToFilterMapping, error) {
	filters := make(map[privdatacommon.DigKey]collectionRoutingFilter)
	for digest, sources := range dig2src {
		anyPeerInCollection, err := p.getLatestCollectionConfigRoutingFilter(digest.Namespace, digest.Collection)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		sources := sources
		endorserPeer, err := p.PeerFilter(common.ChannelID(p.channel), func(peerSignature api.PeerSignature) bool {
			for _, endorsement := range sources {
				endorserbytes, err := proto.Marshal(endorsement.Endorser)
				if err != nil {
					logger.Errorf("Error while marshalling endorserer: %v", err)
					return false
				}
				if bytes.Equal(endorserbytes, []byte(peerSignature.PeerIdentity)) {
					return true
				}
			}
			return false
		})
		if err != nil {
			return nil, errors.WithStack(err)
		}

		filters[digest] = collectionRoutingFilter{
			anyPeer:       anyPeerInCollection,
			preferredPeer: endorserPeer,
		}
	}
	return filters, nil
}
