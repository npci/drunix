/*
Copyright National Payments Corporation of India. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package statesqldb

import (
	"fmt"
	"strings"
	"sync"

	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type iTable interface {
	TableName() string
}

type PrivateTable struct {
	BlockNumber       uint64 `json:"block_number"`
	TransactionNumber uint64 `json:"transaction_number"`

	Key        []byte         `json:"key" gorm:"primaryKey"`
	DBMetadata []byte         `json:"db_metadata"`
	DBValue    datatypes.JSON `json:"db_value"`

	channel string `gorm:"-"`
	name    string `gorm:"-"`
}

func (l PrivateTable) TableName() string {
	return fmt.Sprintf("%s.%s", l.channel, l.name)
}

type HashTable struct {
	BlockNumber       uint64 `json:"block_number"`
	TransactionNumber uint64 `json:"transaction_number"`

	Key        []byte `json:"key" gorm:"primaryKey"`
	DBMetadata []byte `json:"db_metadata"`
	DBValue    []byte `json:"db_value"`

	channel string `gorm:"-"`
	name    string `gorm:"-"`
}

func (l HashTable) TableName() string {
	return fmt.Sprintf("%s.%s", l.channel, l.name)
}

type TableCache interface {
	CreateIfNotExists(key string) error
	Clear()
}
type tableCache struct {
	channel string
	client  *gorm.DB
	mutex   sync.RWMutex
	data    map[string]struct{}
}

func newTableCache(channel string, client *gorm.DB) (TableCache, error) {

	var tables []string
	err := client.Raw(`SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = ?;`, channel).Scan(&tables).Error
	if err != nil {
		return nil, err
	}

	data := make(map[string]struct{}, len(tables))
	for _, table := range tables {
		data[table] = struct{}{}
	}

	return &tableCache{
		channel: channel,
		client:  client,
		mutex:   sync.RWMutex{},
		data:    data,
	}, nil
}

func (c *tableCache) CreateIfNotExists(key string) error {

	if c.exists(key) {
		return nil
	}

	var table iTable
	if strings.Contains(key, "$$h") {
		table = &HashTable{channel: c.channel, name: key}
	} else {
		table = &PrivateTable{channel: c.channel, name: key}
	}

	var tables []string
	err := c.client.Raw(`SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = ? AND tablename = ?;`, c.channel, key).Scan(&tables).Error
	if err != nil {
		return err
	}

	if len(tables) != 0 {
		return nil
	}

	fmt.Printf("Creating table %s\n", table.TableName())

	err = c.client.Table(table.TableName()).AutoMigrate(table)
	if err != nil {
		return fmt.Errorf("error while creating table %s: %v", key, err)
	}

	c.store(key)

	return nil
}

func (c *tableCache) store(key string) {
	c.mutex.Lock()
	c.data[key] = struct{}{}
	c.mutex.Unlock()
}

func (c *tableCache) exists(key string) bool {
	c.mutex.RLock()
	_, ok := c.data[key]
	c.mutex.RUnlock()
	return ok
}

func (c *tableCache) Clear() {
	c.mutex.Lock()
	clear(c.data)
	c.mutex.Unlock()
}
