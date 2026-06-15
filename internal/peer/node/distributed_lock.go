/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/
package node

import (
	crand "crypto/rand"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/redigo"
	redigolib "github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"

	"github.com/spf13/viper"
)

type distributedLockConfig struct {
	enabled                                 bool
	peerID                                  string
	keyValueStoreAddress                    string
	keyValueStorePassword                   string
	keyValueStorePoolMaxIdleConnections     int
	keyValueStorePoolIdleConnectionsTimeout time.Duration
	lockExpiry                              time.Duration
	lockRefreshInterval                     time.Duration
	passiveRetryInterval                    time.Duration
	litePeerEnabled                         bool
}

/*
DRUNIX:

	this method
		- acquires the distributed lock
		- if lock acquired by this service it becomes active committing peer and keep on extending the lock expiry interval
		- if due to any issue this committing peer is crashed the lock expires and the passive committing peer acquires the lock and keeps on extending the lock expiry interval
		- the passive committing peer keeps on trying to acquire lock after every `passiveRetryInterval`
*/
func acquireDistributedLockAndServe(args []string) error {

	config := loadDistributedLockConfig()

	if config.litePeerEnabled || !config.enabled {
		return serve(args)
	}

	if config.keyValueStoreAddress == "" {
		return errors.New("key value store address not found")
	}

	// It is important to have the lock refresh interval less than the lock expiry duration, otherwise lock expires and the peer can't extend the lock.
	if config.lockExpiry < config.lockRefreshInterval {
		return errors.New("lock expiry duration should be greater than lock refresh interval")
	}

	pool := redigo.NewPool(&redigolib.Pool{
		MaxIdle:     config.keyValueStorePoolMaxIdleConnections,
		IdleTimeout: config.keyValueStorePoolIdleConnectionsTimeout,
		Dial: func() (redigolib.Conn, error) {
			return redigolib.Dial("tcp", config.keyValueStoreAddress, redigolib.DialPassword(config.keyValueStorePassword))
		},
		TestOnBorrow: func(c redigolib.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	})

	rs := redsync.New(pool)
	lockName := fmt.Sprintf("acquire-cp-lock-%s", config.peerID)

	// Use the crypto/rand package (crand) to fill the seed array with cryptographically secure random bytes. Initializes a new random number generator instance using the ChaCha8 cipher seeded with the secure random bytes.
	var r *rand.Rand
	var seed [32]byte
	_, _ = crand.Read(seed[:])
	r = rand.New(rand.NewChaCha8(seed))

	lock := rs.NewMutex(lockName, redsync.WithExpiry(config.lockExpiry))
	for {
		randomDeltaDuration := r.Int64N(100)
		// if this committing peer acquires lock it will intialize `Serve()` and concurrently in a go routine after every lock refresh interval it will keep on extending the lock expiry durations.
		if err := lock.Lock(); err == nil {
			logger.Info("Acquired lock, running as active service")
			go func() {
				for {
					// a delta is added to lock refresh interval so that chances of lock contention is reduced when multiple committing peers try to acquire lock at the same time.
					time.Sleep(config.lockRefreshInterval + time.Duration(randomDeltaDuration*int64(time.Millisecond)))
					logger.Debugf("acquire cp lock until: %v\n", lock.Until())
					isExtended, err := lock.Extend()
					// what happens if the extension fails? the lock is free and the passive committing peer acquires the lock and tries to intialize the serve. But the serve is already intialized and its a blocking call. The passice CP got the lock but since the active peer didn't close the serve it has level db lock the passive keeps on crashing.

					// retry for 1 time and if still not able to extend panic the CP and let the passive CP take over.
					// TODO implement proper retry
					if err != nil || !isExtended {
						isExtended, err := lock.Extend()
						if err != nil || !isExtended {
							//DRUNIX: when lock extension retry fails unlock and panic
							lock.Unlock()
							logger.Panicf("Failed to extend lock, isExtended %v with err : %v", isExtended, err)
						}
					}
				}
			}()
			// serve is a blocking call. If serve fails the acquired lock is unlocked.
			err := serve(args)
			if err != nil {
				logger.Errorf("Server Error : %v\n", err)
				return err
			}
			isUnlocked, err := lock.Unlock()
			if err != nil || !isUnlocked {
				logger.Infof("Failed to unlock, isUnlocked %v with err : %+v", isUnlocked, err)
			}
			break
		} else {
			// if this committing peer didn't acquire lock it becomes passive committing peer and it tries to acquire lock for every `passiveRetryInterval`
			logger.Infof("Failed to acquire lock, running as passive service : %+v", err)
			time.Sleep(config.passiveRetryInterval)
		}
	}
	return nil
}

// DRUNIX: initialize distributed lock config
func loadDistributedLockConfig() distributedLockConfig {
	config := distributedLockConfig{
		enabled:                                 viper.GetBool("peer.distributedlock.enabled"),
		peerID:                                  viper.GetString("peer.id"),
		keyValueStoreAddress:                    viper.GetString("peer.kvstore.address"),
		keyValueStorePassword:                   viper.GetString("peer.kvstore.password"),
		keyValueStorePoolMaxIdleConnections:     viper.GetInt("peer.distributedlock.poolmaxidleconnections"),
		keyValueStorePoolIdleConnectionsTimeout: viper.GetDuration("peer.distributedlock.poolidleconnectionstimeout"),
		lockExpiry:                              viper.GetDuration("peer.distributedlock.lockexpiry"),
		lockRefreshInterval:                     viper.GetDuration("peer.distributedlock.lockrefreshfrequency"),
		passiveRetryInterval:                    viper.GetDuration("peer.distributedlock.passiveretryinterval"),
		litePeerEnabled:                         viper.GetBool("peer.litepeer.enabled"),
	}

	if config.lockExpiry == 0 {
		config.lockExpiry = 10 * time.Second
	}

	if config.lockRefreshInterval == 0 {
		config.lockRefreshInterval = 5 * time.Second
	}

	if config.passiveRetryInterval == 0 {
		config.passiveRetryInterval = 7 * time.Second
	}

	if config.keyValueStorePoolMaxIdleConnections == 0 {
		config.keyValueStorePoolMaxIdleConnections = 30
	}

	if config.keyValueStorePoolIdleConnectionsTimeout == 0 {
		config.keyValueStorePoolIdleConnectionsTimeout = 500 * time.Second
	}

	return config
}
