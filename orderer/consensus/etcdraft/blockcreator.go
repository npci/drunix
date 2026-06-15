/*
Copyright IBM Corp. 2017 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0

Modifications Copyright National Payments Corporation of India
*/

package etcdraft

import (
	"github.com/golang/protobuf/proto"
	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/npci/drunix/common/flogging"
	"github.com/npci/drunix/protoutil"
)

// blockCreator holds number and hash of latest block
// so that next block will be created based on it.
type blockCreator struct {
	hash   []byte
	number uint64

	logger *flogging.FabricLogger
}

func (bc *blockCreator) createNextBlock(envs []*cb.Envelope) *cb.Block {
	data := &cb.BlockData{
		Data: make([][]byte, len(envs)),
	}

	var err error
	for i, env := range envs {
		data.Data[i], err = proto.Marshal(env)
		if err != nil {
			bc.logger.Panicf("Could not marshal envelope: %s", err)
		}
	}

	bc.number++

	block := protoutil.NewBlock(bc.number, bc.hash)
	block.Header.DataHash = protoutil.BlockDataHash(data)
	block.Data = data

	bc.hash = protoutil.BlockHeaderHash(block.Header)
	return block
}

/*
DRUNIX:
createNextBlockWithBatch creates a new block with a batch of requests
*/
func (bc *blockCreator) createNextBlockWithBatch(envs *orderer.Batch) *cb.Block {

	data := &cb.BlockData{
		Data: make([][]byte, len(envs.Reqs)),
	}

	var err error
	for i, submitEnvReq := range envs.Reqs {
		// bc.logger.Info("Putting in the block - ", len(submitEnvReq.Payload.Payload))

		data.Data[i], err = proto.Marshal(submitEnvReq.Payload)
		if err != nil {
			bc.logger.Panicf("Could not marshal envelope: %s", err)
		}
	}

	bc.number++

	block := protoutil.NewBlock(bc.number, bc.hash)
	block.Header.DataHash = protoutil.BlockDataHash(data)
	block.Data = data

	bc.hash = protoutil.BlockHeaderHash(block.Header)
	return block
}
