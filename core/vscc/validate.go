/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/

package vscc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/npci/drunix/bccsp"
	"github.com/npci/drunix/common/flogging"
	"github.com/npci/drunix/common/metrics"
	"github.com/npci/drunix/common/policies"
	"github.com/npci/drunix/common/semaphore"
	"github.com/npci/drunix/core/committer/txvalidator/plugin"
	"github.com/npci/drunix/core/committer/txvalidator/v20"
	"github.com/npci/drunix/core/committer/txvalidator/v20/plugindispatcher"
	vir "github.com/npci/drunix/core/committer/txvalidator/v20/valinforetriever"
	validation "github.com/npci/drunix/core/handlers/validation/api"
	"github.com/npci/drunix/core/ledger"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/statedb"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/statedb/statecouchdb"
	"github.com/npci/drunix/core/ledger/kvledger/txmgmt/statedb/statesqldb"
	"github.com/npci/drunix/core/peer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	// keyvaluedatabase "github.com/npci/drunix/common/keyValueDatabase"
)

type VsccValidateServer struct {
	pb.VsccServiceServer

	ledgerConfig                *ledger.Config
	validationPluginsByName     map[string]validation.PluginFactoryAdapter
	policyMgr                   policies.PolicyManagerGetterFunc
	peerEndpoint                string
	cryptoProvider              bccsp.BCCSP
	lifecycleValidatorCommitter plugindispatcher.CollectionAndLifecycleResources
	peerInstance                *peer.Peer
	legacyLifecycleValidation   plugindispatcher.LifecycleResources
	metricsProvider             metrics.Provider
	logger                      *flogging.FabricLogger
	versiondbProvider           statedb.VersionedDBProvider
	validatorPoolSize           int
	// keyValueDBConn              *keyvaluedatabase.KeyValueDBConnection

	//required for InitOnConfigBlock
	channelValidators     map[string]*channelValidator
	channelValidatorsLock sync.RWMutex

	//required for InitOnBlock
	// lock              *sync.Mutex
	// txValidator       map[string]*txvalidator.TxValidatorVSCCAdapter
	// validatedBlockNum sync.Map
}

type channelValidator struct {
	lastLifecycleBlockNumber uint64
	lastValidatedBlockNumber uint64
	txValidator              *txvalidator.TxValidatorVSCCAdapter
	versionDb                statedb.VersionedDB
}

func NewVsccValidateServer(ledgerConfig *ledger.Config, validationPluginsByName map[string]validation.PluginFactoryAdapter, policyMgr policies.PolicyManagerGetterFunc, peerEndpoint string, cryptoProvider bccsp.BCCSP, lifecycleValidatorCommitter plugindispatcher.CollectionAndLifecycleResources, peerInstance *peer.Peer, legacyLifecycleValidation plugindispatcher.LifecycleResources, metricsProvider metrics.Provider, validatorPoolSize int) (*VsccValidateServer, error) {

	var err error
	var vdbProvider statedb.VersionedDBProvider

	// keyValueDBConn, err := keyvaluedatabase.GetKeyValueDBConnection()
	// if err != nil {
	// 	return nil, err
	// }

	if ledgerConfig != nil && ledgerConfig.StateDBConfig.StateDatabase == ledger.CouchDB {
		if vdbProvider, err = statecouchdb.NewVersionedDBProvider(ledgerConfig.StateDBConfig.CouchDB, metricsProvider, nil); err != nil {
			return nil, err
		}
	} else if ledgerConfig.StateDBConfig.StateDatabase == ledger.SqlDB {
		vdbProvider, err = statesqldb.NewVersionedDBProvider(ledgerConfig.StateDBConfig.SqlDB, nil, nil)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("VSCC service not supported")
	}

	vsccValidateServer := &VsccValidateServer{
		ledgerConfig:                ledgerConfig,
		validationPluginsByName:     validationPluginsByName,
		policyMgr:                   policyMgr,
		peerEndpoint:                peerEndpoint,
		cryptoProvider:              cryptoProvider,
		lifecycleValidatorCommitter: lifecycleValidatorCommitter,
		peerInstance:                peerInstance,
		legacyLifecycleValidation:   legacyLifecycleValidation,
		metricsProvider:             metricsProvider,
		logger:                      flogging.MustGetLogger("vscc.validator"),
		versiondbProvider:           vdbProvider,
		validatorPoolSize:           validatorPoolSize,
		// keyValueDBConn:              keyValueDBConn,

		channelValidators:     make(map[string]*channelValidator),
		channelValidatorsLock: sync.RWMutex{},

		// lock:              &sync.Mutex{},
		// txValidator:       make(map[string]*txvalidator.TxValidatorVSCCAdapter),
		// validatedBlockNum: sync.Map{},
	}

	return vsccValidateServer, nil
}

func (v *VsccValidateServer) ProcessVscc(ctx context.Context, req *pb.VsccRequest) (*pb.VsccResponse, error) {

	if len(req.Transactions) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "no transactions found")
	}

	validator, err := v.getValidator(req)
	if err != nil {
		return nil, err
	}

	res := validator.Validate(req)

	calcTps.increment(len(req.Transactions))
	calcTps.messageReceived <- true

	return res, nil
}

func (v *VsccValidateServer) InitializeTxValidator(channelName string, blockNumber uint64) (*channelValidator, error) {

	v.logger.Infof("InitializeTxValidator: %v\n", channelName)

	versionDb, err := v.versiondbProvider.GetDBHandle(channelName, nil)
	if err != nil {
		return nil, err
	}

	channel, err := v.peerInstance.InitializeChannel(channelName, v.cryptoProvider, versionDb)
	if err != nil {
		return nil, err
	}

	txValidator, err := txvalidator.NewTxValidatorVSCCAdapter(
		channelName,
		semaphore.New(v.validatorPoolSize),
		channel,
		plugin.MapBasedMapperAdapter(v.validationPluginsByName),
		v.policyMgr,
		v.cryptoProvider,
		v.peerEndpoint,
		versionDb,
		&peer.CollectionInfoShim{
			CollectionAndLifecycleResources: v.lifecycleValidatorCommitter,
			ChannelID:                       channelName,
		},
		&vir.ValidationInfoRetrieveShim{
			New:    v.lifecycleValidatorCommitter,
			Legacy: v.legacyLifecycleValidation,
		},
		v.metricsProvider,
	)

	if err != nil {
		return nil, err
	}

	txValidator.LeanEnabled = channel.LeanEnabled

	if blockNumber == 0 {
		versionedValue, err := versionDb.GetState("", "LAST_LIFECYCLE_BLOCK_NUMBER")
		if err != nil {
			v.logger.Errorf("failed to initilialize txValidator : %v", err)
			return nil, status.Errorf(codes.Internal, "failed to initilialize txValidator")
		}
		blockNumber = versionedValue.Version.BlockNum
	}

	return &channelValidator{
		lastLifecycleBlockNumber: blockNumber,
		txValidator:              txValidator,
		versionDb:                versionDb,
	}, nil
}

/*
DRUNIX
Redis PubSub Consumer which listens to the config block updates in CP,
and re-initialize the validator on receiving events.
*/
// func (v *VsccValidateServer) ConfigTxConsumer() {

// 	ctx := context.Background()

// 	sub, err := v.keyValueDBConn.Subscribe(ctx, "config-tx-channel")
// 	if err != nil {
// 		panic(err)
// 	}

// 	defer sub.Close()

// 	/*
// 	   DRUNIX
// 	   Removed time.After() which re-initializes the counter multiple times
// 	*/
// 	timeout := time.NewTimer(v.ledgerConfig.VsccConfig.ConfigTxChannelTimeout)
// 	defer timeout.Stop()

// 	for {
// 		select {
// 		case msg, ok := <-sub.Channel():
// 			if !ok {
// 				log.Fatal("Subscription channel closed")
// 			}
// 			v.logger.Info("Received Config Tx Msg : ", msg.Payload)
// 			v.lock.Lock()
// 			err := v.InitializeTxValidator(msg.Payload)
// 			v.lock.Unlock()
// 			if err != nil {
// 				log.Fatalf("Failed to initialize TxValidator: %v", err)
// 			}
// 			if !timeout.Stop() {
// 				<-timeout.C
// 			}
// 			timeout.Reset(v.ledgerConfig.VsccConfig.ConfigTxChannelTimeout)
// 		case <-ctx.Done():
// 			log.Fatal("Context canceled")

// 		/*
// 			DRUNIX
// 			PubSub timeout is blocking the loop,
// 			so using redis ping for health check
// 		*/
// 		case <-timeout.C:
// 			err := v.keyValueDBConn.Ping(ctx)
// 			if err != nil {
// 				log.Fatal("keyValueDB connection lost")
// 			}
// 			timeout.Reset(v.ledgerConfig.VsccConfig.ConfigTxChannelTimeout)
// 		}
// 	}
// }

var calcTps CalcTps

type CalcTps struct {
	txnCounter      atomic.Uint64
	startTime       time.Time
	endTime         time.Time
	messageReceived chan bool
}

func (c *CalcTps) increment(txnCount int) {
	c.txnCounter.Add(uint64(txnCount))
}

func init() {

	calcTps = CalcTps{
		txnCounter:      atomic.Uint64{},
		startTime:       time.Time{},
		endTime:         time.Time{},
		messageReceived: make(chan bool),
	}

	go func() {
		timer := time.NewTimer(30 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-calcTps.messageReceived:
				if calcTps.startTime.IsZero() {
					calcTps.startTime = time.Now()
				}
				calcTps.endTime = time.Now()
				timer.Reset(30 * time.Second)

			case <-timer.C:

				if !calcTps.startTime.IsZero() {
					duration := calcTps.endTime.Sub(calcTps.startTime)
					fmt.Println("Start Time    : ", calcTps.startTime)
					fmt.Println("End Time      : ", calcTps.endTime)
					fmt.Println("Txn Count     : ", calcTps.txnCounter.Load())
					fmt.Println("Time Taken    : ", duration)
					fmt.Println("TPS           : ", float64(calcTps.txnCounter.Load())/float64(duration.Seconds()))
					calcTps.startTime = time.Time{}
					calcTps.txnCounter.Store(0)
				}
				timer.Reset(30 * time.Second)
			}
		}
	}()
}

// func (v *VsccValidateServer) initializeValidatorOnBlock(req *pb.VsccRequest) (*txvalidator.TxValidatorVSCCAdapter, error) {

// 	//TODO :- extract and benchmark with O(n^2) with x/syncMap
// 	// DRUNIX : if the block number in the request and the previously processed block number are not same re-initialize the transaction validator.
// 	// TODO : comparision should happen on hash
// 	blockNum, channelExsists := v.validatedBlockNum.Load(req.ChannelId)
// 	if !channelExsists {
// 		blockNum = 0
// 	}
// 	blockNumInReq := req.BlockNum

// 	if blockNum != blockNumInReq {
// 		v.lock.Lock()
// 		// DRUNIX : need a second check because there is a chance that another go routine tries to re-initialize already intialized object.
// 		blockNum, channelExsists := v.validatedBlockNum.Load(req.ChannelId)
// 		if !channelExsists {
// 			blockNum = 0
// 		}
// 		if blockNum != blockNumInReq {
// 			channelValidator, err := v.InitializeTxValidator(req.ChannelId, uint64(req.BlockNum))
// 			if err != nil {
// 				v.lock.Unlock()
// 				v.logger.Errorf("failed to initilialize txValidator : %v", err)
// 				return nil, status.Errorf(codes.Internal, "failed to initilialize txValidator")
// 			}
// 			v.validatedBlockNum.Store(req.ChannelId, req.BlockNum)
// 			v.txValidator[req.ChannelId] = channelValidator.txValidator
// 			fmt.Println("************** initialised validator for block : ", req.BlockNum)
// 		}
// 		v.lock.Unlock()
// 	}
// 	return v.txValidator[req.ChannelId], nil
// }

func (v *VsccValidateServer) getValidator(req *pb.VsccRequest) (*txvalidator.TxValidatorVSCCAdapter, error) {

	v.channelValidatorsLock.Lock()
	if channelValidator, exists := v.channelValidators[req.ChannelId]; !exists {

		channelValidator, err := v.InitializeTxValidator(req.ChannelId, 0)
		if err != nil {
			v.logger.Errorf("failed to initilialize txValidator : %v", err)
			return nil, status.Errorf(codes.Internal, "failed to initilialize txValidator")
		}
		v.channelValidators[req.ChannelId] = channelValidator
		v.channelValidators[req.ChannelId].lastValidatedBlockNumber = uint64(req.BlockNum)

	} else {

		if channelValidator.lastValidatedBlockNumber != uint64(req.BlockNum) {

			versionedValue, err := channelValidator.versionDb.GetState("", "LAST_LIFECYCLE_BLOCK_NUMBER")
			if err != nil {
				v.logger.Errorf("failed to initilialize txValidator : %v", err)
				return nil, status.Errorf(codes.Internal, "failed to initilialize txValidator")
			}
			lastLifecycleBlockNumberInDB := versionedValue.Version.BlockNum

			if channelValidator.lastLifecycleBlockNumber != lastLifecycleBlockNumberInDB {

				channelValidator, err = v.InitializeTxValidator(req.ChannelId, lastLifecycleBlockNumberInDB)
				if err != nil {
					v.logger.Errorf("failed to initilialize txValidator : %v", err)
					return nil, status.Errorf(codes.Internal, "failed to initilialize txValidator")
				}
				v.channelValidators[req.ChannelId] = channelValidator
			}
			v.channelValidators[req.ChannelId].lastValidatedBlockNumber = uint64(req.BlockNum)
		}
	}
	v.channelValidatorsLock.Unlock()

	return v.channelValidators[req.ChannelId].txValidator, nil
}
