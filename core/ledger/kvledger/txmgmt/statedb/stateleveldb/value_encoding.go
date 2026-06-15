/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
Modifications Copyright National Payments Corporation of India
*/

package stateleveldb

import (
	proto "github.com/golang/protobuf/proto"
	"github.com/npci/drunix/core/ledger/internal/version"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/statedb"
)

// encodeValue encodes the value, version, and metadata
func encodeValue(v *statedb.VersionedValue) ([]byte, error) {
	return proto.Marshal(
		&DBValue{
			Version:  v.Version.ToBytes(),
			Value:    v.Value,
			Metadata: v.Metadata,
		},
	)
}

func encodeValueMVCC(v *statedb.VersionMVCC) ([]byte, error) {
	return proto.Marshal(
		&DBMVCC{
			Version:  v.Version.ToBytes(),
			IsDelete: v.Is_Delete,
		},
	)
}

// decodeValue decodes the statedb value bytes
func decodeValue(encodedValue []byte) (*statedb.VersionedValue, error) {
	dbValue := &DBValue{}
	err := proto.Unmarshal(encodedValue, dbValue)
	if err != nil {
		return nil, err
	}
	ver, _, err := version.NewHeightFromBytes(dbValue.Version)
	if err != nil {
		return nil, err
	}
	val := dbValue.Value
	metadata := dbValue.Metadata
	// protobuf always makes an empty byte array as nil
	if val == nil {
		val = []byte{}
	}
	return &statedb.VersionedValue{Version: ver, Value: val, Metadata: metadata}, nil
}

// decodeValue decodes the statedb value bytes
func decodeValueMVCC(encodedValue []byte) (*statedb.VersionMVCC, error) {
	dbValue := &DBMVCC{}
	err := proto.Unmarshal(encodedValue, dbValue)
	if err != nil {
		return nil, err
	}
	ver, _, err := version.NewHeightFromBytes(dbValue.Version)
	if err != nil {
		return nil, err
	}
	return &statedb.VersionMVCC{Version: ver, Is_Delete: dbValue.GetIsDelete()}, nil
}
