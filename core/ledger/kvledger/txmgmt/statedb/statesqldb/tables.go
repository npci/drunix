/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/

package statesqldb

import (
	"fmt"
	"strings"
)

type Lifecycle struct {
	BlockNumber       uint64 `json:"block_number"`
	TransactionNumber uint64 `json:"transaction_number"`

	Key        []byte `json:"key" gorm:"primaryKey"`
	DBMetadata []byte `json:"db_metadata"`
	DBValue    []byte `json:"db_value"`

	channel string `gorm:"-"`
	peerId  string `gorm:"-"`
}

func (l Lifecycle) TableName() string {
	return fmt.Sprintf("%s.%s_lifecycle", l.channel, strings.ToLower(strings.ReplaceAll(l.peerId, ".", "_")))
}
