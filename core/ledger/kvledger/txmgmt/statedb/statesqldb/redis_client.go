/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/


package statesqldb

import (
	"context"
	"fmt"

	"time"

	redis "github.com/redis/go-redis/v9"
)

type redisClient struct {
	Client        *redis.Client
	KeyExpiryTime time.Duration
	ReplicaClient *redis.Client
}

type redisSchema struct {
	*redisClient
	schema string
	peerId string
}

type RedisClient interface {
	NewSchema(schema string, peerId string) RedisSchema
}

func (r *redisClient) NewSchema(schema string, peerId string) RedisSchema {
	return &redisSchema{
		redisClient: r,
		schema:      schema,
		peerId:      peerId,
	}
}

type RedisSchema interface {
	SetCert(key string, value []byte) error
	SetHeight(height uint64) error
}

func (r *redisSchema) SetCert(key string, value []byte) error {
	return r.Client.Set(context.Background(), key, value, 0).Err()
}

func (r *redisSchema) SetHeight(height uint64) error {
	return r.Client.ZAdd(context.Background(), fmt.Sprintf("%s:heights", r.schema), redis.Z{
		Score:  float64(height),
		Member: r.peerId,
	}).Err()
}
