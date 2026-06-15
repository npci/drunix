/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/

package node

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	cb "github.com/hyperledger/fabric-protos-go/common"
	"github.com/npci/drunix/bccsp/factory"
	"github.com/npci/drunix/common/crypto"
	"github.com/npci/drunix/common/fabhttp"
	"github.com/npci/drunix/common/flogging"
	floggingmetrics "github.com/npci/drunix/common/flogging/metrics"
	"github.com/npci/drunix/common/grpclogging"
	"github.com/npci/drunix/common/grpcmetrics"
	keyvaluedatabase "github.com/npci/drunix/common/keyValueDatabase"
	"github.com/npci/drunix/common/metadata"
	"github.com/npci/drunix/common/policies"
	"github.com/npci/drunix/core/chaincode"
	"github.com/npci/drunix/core/chaincode/extcc"
	"github.com/npci/drunix/core/chaincode/lifecycle"
	"github.com/npci/drunix/core/chaincode/persistence"
	"github.com/npci/drunix/core/common/ccprovider"
	coreconfig "github.com/npci/drunix/core/config"
	"github.com/npci/drunix/core/container"
	"github.com/npci/drunix/core/container/externalbuilder"
	"github.com/npci/drunix/core/deliverservice"
	"github.com/npci/drunix/core/endorser"
	"github.com/npci/drunix/core/handlers/library"
	validation "github.com/npci/drunix/core/handlers/validation/api"
	"github.com/npci/drunix/core/operations"
	"github.com/npci/drunix/core/peer"
	"github.com/npci/drunix/core/scc/lscc"
	"github.com/npci/drunix/core/transientstore"
	"github.com/npci/drunix/core/vscc"
	gossipprivdata "github.com/npci/drunix/gossip/privdata"
	"github.com/npci/drunix/internal/peer/version"
	"github.com/npci/drunix/internal/pkg/comm"
	"github.com/npci/drunix/msp"
	"github.com/npci/drunix/msp/mgmt"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v2"
)

var chaincodeDevMode bool

func startCmd() *cobra.Command {
	// Set the flags on the node start command.
	flags := nodeStartCmd.Flags()
	flags.BoolVarP(&chaincodeDevMode, "peer-chaincodedev", "", false, "start peer in chaincode development mode")
	return nodeStartCmd
}

var nodeStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Starts the node.",
	Long:  `Starts a node that interacts with the network.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) != 0 {
			return fmt.Errorf("trailing args detected")
		}
		// Parsing of the command line is done so silence cmd usage
		cmd.SilenceUsage = true
		return serve(args)
	},
}

// externalVMAdapter adapts coerces the result of Build to the
// container.Interface type expected by the VM interface.
type externalVMAdapter struct {
	detector *externalbuilder.Detector
}

func (e externalVMAdapter) Build(
	ccid string,
	mdBytes []byte,
	codePackage io.Reader,
) (container.Instance, error) {
	i, err := e.detector.Build(ccid, mdBytes, codePackage)
	if err != nil {
		return nil, err
	}

	// ensure <nil> is returned instead of (*externalbuilder.Instance)(nil)
	if i == nil {
		return nil, nil
	}
	return i, err
}

type disabledDockerBuilder struct{}

func (disabledDockerBuilder) Build(string, *persistence.ChaincodePackageMetadata, io.Reader) (container.Instance, error) {
	return nil, errors.New("docker build is disabled")
}

type endorserChannelAdapter struct {
	peer *peer.Peer
}

func (e endorserChannelAdapter) Channel(channelID string) *endorser.Channel {
	if peerChannel := e.peer.Channel(channelID); peerChannel != nil {
		return &endorser.Channel{
			IdentityDeserializer: peerChannel.MSPManager(),
		}
	}

	return nil
}

type custodianLauncherAdapter struct {
	launcher      chaincode.Launcher
	streamHandler extcc.StreamHandler
}

func (c custodianLauncherAdapter) Launch(ccid string) error {
	return c.launcher.Launch(ccid, c.streamHandler)
}

func (c custodianLauncherAdapter) Stop(ccid string) error {
	return c.launcher.Stop(ccid)
}

func serve(args []string) error {
	logger.Infof("Starting %s", version.GetInfo())

	// Info logging for peer config, includes core.yaml settings and environment variable overrides
	allSettings := viper.AllSettings()
	settingsYaml, err := yaml.Marshal(allSettings)
	if err != nil {
		return err
	}
	logger.Infof("Peer config with combined core.yaml settings and environment variable overrides:\n%s", settingsYaml)

	// Debug logging for peer environment variables
	logger.Debugf("Environment variables:")
	envVars := os.Environ()
	for _, envVar := range envVars {
		logger.Debug(envVar)
	}

	// currently the peer only works with the standard MSP
	// because in certain scenarios the MSP has to make sure
	// that from a single credential you only have a single 'identity'.
	// Idemix does not support this *YET* but it can be easily
	// fixed to support it. For now, we just make sure that
	// the peer only comes up with the standard MSP
	mspType := mgmt.GetLocalMSP(factory.GetDefault()).GetType()
	if mspType != msp.FABRIC {
		panic("Unsupported msp type " + msp.ProviderTypeToString(mspType))
	}

	// Trace RPCs with the golang.org/x/net/trace package. This was moved out of
	// the deliver service connection factory as it has process wide implications
	// and was racy with respect to initialization of gRPC clients and servers.
	grpc.EnableTracing = true

	// obtain coreConfiguration
	coreConfig, err := peer.GlobalConfig()
	if err != nil {
		return err
	}

	opsSystem := newOperationsSystem(coreConfig)
	err = opsSystem.Start()
	if err != nil {
		return errors.WithMessage(err, "failed to initialize operations subsystem")
	}
	defer opsSystem.Stop()

	metricsProvider := opsSystem.Provider
	logObserver := floggingmetrics.NewObserver(metricsProvider)
	flogging.SetObserver(logObserver)

	chaincodeInstallPath := filepath.Join(coreconfig.GetPath("peer.fileSystemPath"), "lifecycle", "chaincodes")
	ccStore := persistence.NewStore(chaincodeInstallPath)
	ccPackageParser := &persistence.ChaincodePackageParser{
		MetadataProvider: ccprovider.PersistenceAdapter(ccprovider.MetadataAsTarEntries),
	}

	listenAddr := coreConfig.ListenAddress
	serverConfig, err := peer.GetServerConfig()
	if err != nil {
		logger.Fatalf("Error loading secure config for peer (%s)", err)
	}

	serverConfig.Logger = flogging.MustGetLogger("core.comm").With("server", "PeerServer")
	serverConfig.ServerStatsHandler = comm.NewServerStatsHandler(metricsProvider)
	serverConfig.UnaryInterceptors = append(
		serverConfig.UnaryInterceptors,
		grpcmetrics.UnaryServerInterceptor(grpcmetrics.NewUnaryMetrics(metricsProvider)),
		grpclogging.UnaryServerInterceptor(flogging.MustGetLogger("comm.grpc.server").Zap()),
	)
	serverConfig.StreamInterceptors = append(
		serverConfig.StreamInterceptors,
		grpcmetrics.StreamServerInterceptor(grpcmetrics.NewStreamMetrics(metricsProvider)),
		grpclogging.StreamServerInterceptor(flogging.MustGetLogger("comm.grpc.server").Zap()),
	)

	semaphores := initGrpcSemaphores(coreConfig)
	if len(semaphores) != 0 {
		serverConfig.UnaryInterceptors = append(serverConfig.UnaryInterceptors, unaryGrpcLimiter(semaphores))
		serverConfig.StreamInterceptors = append(serverConfig.StreamInterceptors, streamGrpcLimiter(semaphores))
	}

	cs := comm.NewCredentialSupport()
	if serverConfig.SecOpts.UseTLS {
		logger.Info("Starting peer with TLS enabled")
		cs = comm.NewCredentialSupport(serverConfig.SecOpts.ServerRootCAs...)

		// set the cert to use if client auth is requested by remote endpoints
		clientCert, err := peer.GetClientCertificate()
		if err != nil {
			logger.Fatalf("Failed to set TLS client certificate (%s)", err)
		}
		cs.SetClientCertificate(clientCert)
	}

	// DRUNIX: initialize key-value db connection
	err = keyvaluedatabase.NewKeyValueDBConnection()
	if err != nil {
		return errors.WithMessage(err, "failed to initialise key-value db for kv store")
	}

	transientStoreProvider, err := transientstore.NewStoreProvider(
		filepath.Join(coreconfig.GetPath("peer.fileSystemPath"), "transientstore"),
	)
	if err != nil {
		return errors.WithMessage(err, "failed to open transient store")
	}

	deliverServiceConfig := deliverservice.GlobalConfig()

	peerInstance := &peer.Peer{
		ServerConfig:             serverConfig,
		CredentialSupport:        cs,
		StoreProvider:            transientStoreProvider,
		CryptoProvider:           factory.GetDefault(),
		OrdererEndpointOverrides: deliverServiceConfig.OrdererEndpointOverrides,
	}

	localMSP := mgmt.GetLocalMSP(factory.GetDefault())

	signingIdentity, err := localMSP.GetDefaultSigningIdentity()
	if err != nil {
		logger.Panicf("Could not get the default signing identity from the local MSP: [%+v]", err)
	}
	signingIdentityBytes, err := signingIdentity.Serialize()
	if err != nil {
		logger.Panicf("Failed to serialize the signing identity: %v", err)
	}

	expirationLogger := flogging.MustGetLogger("certmonitor")
	crypto.TrackExpiration(
		serverConfig.SecOpts.UseTLS,
		serverConfig.SecOpts.Certificate,
		cs.GetClientCertificate().Certificate,
		signingIdentityBytes,
		expirationLogger.Infof,
		expirationLogger.Warnf, // This can be used to piggyback a metric event in the future
		time.Now(),
		time.AfterFunc,
	)

	policyMgr := policies.PolicyManagerGetterFunc(peerInstance.GetPolicyManager)

	// TODO, unfortunately, the lifecycle initialization is very unclean at the
	// moment. This is because ccprovider.SetChaincodePath only works after
	// ledgermgmt.Initialize, but ledgermgmt.Initialize requires a reference to
	// lifecycle.  Finally, lscc requires a reference to the system chaincode
	// provider in order to be created, which requires chaincode support to be
	// up, which also requires, you guessed it, lifecycle. Once we remove the
	// v1.0 lifecycle, we should be good to collapse all of the init of lifecycle
	// to this point.
	lifecycleResources := &lifecycle.Resources{
		Serializer:          &lifecycle.Serializer{},
		ChannelConfigSource: peerInstance,
		ChaincodeStore:      ccStore,
		PackageParser:       ccPackageParser,
	}

	privdataConfig := gossipprivdata.GlobalConfig()
	lifecycleValidatorCommitter := &lifecycle.ValidatorCommitter{
		CoreConfig:                   coreConfig,
		PrivdataConfig:               privdataConfig,
		Resources:                    lifecycleResources,
		LegacyDeployedCCInfoProvider: &lscc.DeployedCCInfoProvider{},
	}

	peerServer, err := comm.NewGRPCServer(listenAddr, serverConfig)
	if err != nil {
		logger.Fatalf("Failed to create peer server (%s)", err)
	}

	logger.Debugf("Running peer")

	libConf, err := library.LoadConfig()
	if err != nil {
		return errors.WithMessage(err, "could not decode peer handlers configuration")
	}

	//DRUNIX : changed vscc validation handler in config from DefaultValidation to DefaultValidationAdapter
	libConf.Validators["vscc"].Name = "DefaultValidationAdapter"

	reg := library.InitRegistryAdapter(libConf)

	validationPluginsByName := reg.Lookup(library.Validation).(map[string]validation.PluginFactoryAdapter)

	logger.Infof("Starting peer with ID=[%s], network ID=[%s], address=[%s]", coreConfig.PeerID, coreConfig.NetworkID, coreConfig.PeerAddress)

	// Get configuration before starting go routines to avoid
	// racing in tests
	profileEnabled := coreConfig.ProfileEnabled
	profileListenAddress := coreConfig.ProfileListenAddress

	// Start the grpc server. Done in a goroutine so we can deploy the
	// genesis block if needed.
	serve := make(chan error)

	// Start profiling http endpoint if enabled
	if profileEnabled {
		go func() {
			logger.Infof("Starting profiling server with listenAddress = %s", profileListenAddress)
			if profileErr := http.ListenAndServe(profileListenAddress, nil); profileErr != nil {
				logger.Errorf("Error starting profiler: %s", profileErr)
			}
		}()
	}

	logger.Infof("Started peer with ID=[%s], network ID=[%s], address=[%s]", coreConfig.PeerID, coreConfig.NetworkID, coreConfig.PeerAddress)

	vsccValidateServer, err := vscc.NewVsccValidateServer(
		ledgerConfig(),
		validationPluginsByName,
		policyMgr,
		coreConfig.PeerAddress,
		factory.GetDefault(),
		lifecycleValidatorCommitter,
		peerInstance,
		lifecycleValidatorCommitter,
		metricsProvider,
		coreConfig.ValidatorPoolSize,
	)
	if err != nil {
		panic(err)
	}

	// register service here!
	cb.RegisterVsccServiceServer(peerServer.Server(), vsccValidateServer)

	go func() {
		var grpcErr error
		if grpcErr = peerServer.Start(); grpcErr != nil {
			grpcErr = fmt.Errorf("grpc server exited with error: %s", grpcErr)
		}
		serve <- grpcErr
	}()

	// Block until grpc server exits
	return <-serve
}

func newOperationsSystem(coreConfig *peer.Config) *operations.System {
	return operations.NewSystem(operations.Options{
		Options: fabhttp.Options{
			Logger:        flogging.MustGetLogger("peer.operations"),
			ListenAddress: coreConfig.OperationsListenAddress,
			TLS: fabhttp.TLS{
				Enabled:            coreConfig.OperationsTLSEnabled,
				CertFile:           coreConfig.OperationsTLSCertFile,
				KeyFile:            coreConfig.OperationsTLSKeyFile,
				ClientCertRequired: coreConfig.OperationsTLSClientAuthRequired,
				ClientCACertFiles:  coreConfig.OperationsTLSClientRootCAs,
			},
		},
		Metrics: operations.MetricsOptions{
			Provider: coreConfig.MetricsProvider,
			Statsd: &operations.Statsd{
				Network:       coreConfig.StatsdNetwork,
				Address:       coreConfig.StatsdAaddress,
				WriteInterval: coreConfig.StatsdWriteInterval,
				Prefix:        coreConfig.StatsdPrefix,
			},
		},
		Version: metadata.Version,
	})
}
