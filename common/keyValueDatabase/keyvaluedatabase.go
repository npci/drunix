/*
Copyright National Payments Corporation of India. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package keyvaluedatabase

import (
	"context"
	"fmt"
	"sync"

	"time"

	"github.com/npci/drunix/common/flogging"
	"github.com/pkg/errors"
	redis "github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
)

/*
DRUNIX:
	This package initializes a new key-value db instance.
	This instance will be used as transient store and to publish configuration transaction notification for VSCC service
*/

var logger = flogging.MustGetLogger("keyvaluedb")

var CertCache = &sync.Map{}

type KeyValueDBConfig struct {
	Address         string
	Password        string
	ConnMaxIdleTime time.Duration
	PoolFIFO        bool
	PoolSize        int
	MinIdleConns    int
	MaxIdleConns    int
	DB              int
	ReplicaAddress  string
	PeerId          string
}

type KeyValueDBConnection struct {
	Client        *redis.Client
	ReplicaClient *redis.Client
	PeerId        string
}

var keyValueDBConn *KeyValueDBConnection

func GetKeyValueDBConnection() (*KeyValueDBConnection, error) {
	if keyValueDBConn == nil {
		return nil, fmt.Errorf("key-value database connection is not initialised.")
	}
	return keyValueDBConn, nil
}

func NewKeyValueDBConnection() error {

	redisOptions := redis.Options{
		Addr:            viper.GetString("peer.kvstore.address"),
		Password:        viper.GetString("peer.kvstore.password"),
		PoolFIFO:        viper.GetBool("peer.kvstore.poolfifo"),
		PoolSize:        viper.GetInt("peer.kvstore.poolsize"),
		MinIdleConns:    viper.GetInt("peer.kvstore.minidleconns"),
		MaxIdleConns:    viper.GetInt("peer.kvstore.maxidleconns"),
		MaxActiveConns:  viper.GetInt("peer.kvstore.maxactiveconns"),
		ConnMaxIdleTime: viper.GetDuration("peer.kvstore.connmaxidletime"),
		DB:              viper.GetInt("peer.kvstore.db"),
	}

	client := redis.NewClient(&redisOptions)

	redisOptions.Addr = viper.GetString("peer.kvstore.replicaaddress")

	replicaClient := redis.NewClient(&redisOptions)

	masterPing, err := client.Ping(context.Background()).Result()
	if err != nil {
		return err
	}

	logger.Info("the key-value store ping:", masterPing)

	replicaPing, err := replicaClient.Ping(context.Background()).Result()
	if err != nil {
		return err
	}

	logger.Info("the replica key-value store ping:", replicaPing)

	peerid := viper.GetString("peer.id")
	if peerid == "" {
		return errors.New("peer.id environment variable should not be nil")
	}

	keyValueDBConn = &KeyValueDBConnection{
		Client:        client,
		ReplicaClient: replicaClient,
		PeerId:        peerid,
	}

	return nil
}
