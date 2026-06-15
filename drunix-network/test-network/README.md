# Running the test network

You can use the `./network.sh` script to stand up a simple drunix test network. The test network has two peer organizations with one committing peer, one lite peer and one vssc server each and a single node raft ordering service. You can also use the `./network.sh` script to create channels, deploy chaincode, and run common chaincode lifecycle operations.

## Prerequisites

- Linux: git, Docker, Golang, jq

Default values are defined in `network.config`. By default, the script uses:

- Channel name: `mychannel`
- Crypto material generation: `cryptogen`
- Database: `yugabyte`
- Chaincode language: `go`
- Chaincode name: `basic`
- Chaincode path: `../asset-transfer-basic/chaincode-go`
- Chaincode version: `1.0`
- Chaincode sequence: `auto`
- Org for chaincode commands: `1`

## Network commands

To create a network use:

```bash
./network.sh up
```

To create a channel use:

```bash
./network.sh createChannel
```

Note that running the `createChannel` command will start the network, if it is not already running.

You can also bring up the network and create a channel in one command:

```bash
./network.sh up createChannel
```

To restart a running network use:

```bash
./network.sh restart
```

To stop and clean up the network use:

```bash
./network.sh down
```

To install required binaries and images use:

```bash
./network.sh prereq
```

## Common flags

The following flags are supported by `up` and `createChannel`:

```bash
-ca             Use Certificate Authorities to generate crypto material
-cfssl          Use CFSSL to generate crypto material
-c <name>       Channel name, defaults to mychannel
-s <database>   Database type, defaults to yugabyte
-r <number>     Max retry attempts, defaults to 5
-d <seconds>    CLI delay in seconds, defaults to 3
-verbose        Enable verbose output
```

Example:

```bash
./network.sh up createChannel -c mychannel -ca -r 5 -d 3 -s yugabyte
```

## Deploying chaincode

To deploy a chaincode in the network use:

```bash
./network.sh deployCC -ccn <chaincode-name> -ccp <chaincode-path> -ccl <chaincode-language>
```

Common deploy flags:

```bash
-c <channel>                  Channel name
-ccn <name>                   Chaincode name
-ccl <language>               Chaincode language: go, java, javascript, typescript
-ccv <version>                Chaincode version, defaults to 1.0
-ccs <sequence>               Chaincode sequence, defaults to auto
-ccp <path>                   Chaincode source path
-ccep <policy>                Optional endorsement policy
-cccg <collection-config>     Optional private data collection config path
-cci <function>               Optional chaincode init function
```

## Creating collection configs

Private data collections are defined in a collection config JSON file. A sample file is available at `drunix-network/asset-transfer-private-data/chaincode-go/collections_config.json`.

Each collection object should include a unique `name` and can include the usual collection policy fields, for example:

```json
[
 {
   "name": "assetCollection",
   "policy": "OR('Org1MSP.member', 'Org2MSP.member')",
   "requiredPeerCount": 1,
   "maxPeerCount": 1,
   "blockToLive":1000000,
   "memberOnlyRead": true,
   "memberOnlyWrite": true,
   "endorsementPolicy": {
    "signaturePolicy":"OR('Org1MSP.member','Org2MSP.member')"
  }   
},
 {
   "name": "Org1MSPPrivateCollection",
   "policy": "OR('Org1MSP.member')",
   "requiredPeerCount": 0,
   "maxPeerCount": 1,
   "blockToLive":3,
   "memberOnlyRead": true,
   "memberOnlyWrite": false,
   "endorsementPolicy": {
     "signaturePolicy": "OR('Org1MSP.member')"
   }
 },
 {
   "name": "Org2MSPPrivateCollection",
   "policy": "OR('Org2MSP.member')",
   "requiredPeerCount": 0,
   "maxPeerCount": 1,
   "blockToLive":3,
   "memberOnlyRead": true,
   "memberOnlyWrite": false,
   "endorsementPolicy": {
     "signaturePolicy": "OR('Org2MSP.member')"
   }
  }
]

```

Update the collection names and policies based on the chaincode's private data needs.

Use the same collection config file while deploying chaincode with -cccg

```
./network.sh deployCC -ccn private-data -ccp ../asset-transfer-private-data/chaincode-go -ccl go -cccg ../asset-transfer-private-data/chaincode-go/collections_config.json
 
```


## Chaincode lifecycle and invoke/query commands

The `cc` mode supports packaging, listing, invoking, and querying chaincode.

Package chaincode:

```bash
./network.sh cc package -ccn basic -ccp ../asset-transfer-basic/chaincode-go -ccv 1.0 -ccl go
```

List installed and committed chaincodes:

```bash
./network.sh cc list -org 1
```

Invoke chaincode:

```bash
./network.sh cc invoke -c mychannel -ccn basic -ccic '{"Args":["InitLedger"]}'
```

Query chaincode:

```bash
./network.sh cc query -c mychannel -ccn basic -ccqc '{"Args":["GetAllAssets"]}'
```

<!-- Before you can deploy the test network, you need to follow the instructions to install the required binaries and container images. -->

## Using the Peer commands

The `envVar.sh` script can be used to set up the environment variables for the organizations, this will help to be able to use the `peer` commands directly.

First, ensure that the peer binaries are on your path, and the Config path is set assuming that you're in the `test-network` directory.

```bash
 export PATH=$PATH:$(realpath ../bin)
 export FABRIC_CFG_PATH=$(realpath ../config)
```

You can then set up the environment variables for each organization. The `envVar.sh` command is designed to be run as follows.

```bash
source ./scripts/envVar.sh
setGlobals orgIndex peerIndex
```
Peer Indices are configured as below 
0 - Lite Peer
1 - Committing Peer

(Note bash v4 is required for the scripts.)

You will now be able to run the `peer` commands in the context of required Org. 

