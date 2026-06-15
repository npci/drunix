/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/

package consts

/*
DRUNIX:
	initialize custom fileds from env
*/

import (
	"fmt"
	"os"
	"strconv"
)

var (
	// //endorser batch config params
	ENDORSER_BATCH_INTERVAL       = getIntEnvDefault("CORE_PEER_ENDORSERBATCHINTERVAL", 100)
	ENDORSER_BATCH_CHANNEL_BUFFER = getIntEnvDefault("CORE_PEER_ENDORSERBATCHCHANNELBUFFER", 2000)

	// //pvt data distribution enable/disable param
	PRIVATEDATA_DESSIMINATION_ENABLED = getBoolEnvDefault("PEER_PRIVATEDATA_DESSIMINATION_ENABLED", true)

	// //store certificates in db so that in payload is light in weight
	CERTINDB = getBoolEnvDefault("PEER_CERT_IN_DB", false)

	MSPID = getEnvDefault("CORE_PEER_LOCALMSPID", "Org1Msp")

	CORE_PEER_ADMIN_KEYSTORE = getEnvDefault("CORE_PEER_ADMIN_KEYSTORE", "/etc/hyperledger/users/Admin@org2.example.com/msp/keystore")

	PEER_ADMIN_SIGN_CERT = getEnvDefault("PEER_ADMIN_SIGN_CERT", "/etc/hyperledger/users/Admin@org2.example.com/msp/signcerts/Admin@org2.example.com-cert.pem")

	// PEER_MSP_SIGNCERTS = getEnvDefault("PEER_MSP_SIGNCERTS", "/etc/hyperledger/fabric/msp/signcerts/peer0.org2.example.com-cert.pem")
)

func getEnvDefault(key, defaultVal string) string {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	return val
}

// func getInt64EnvDefault(key string, defaultVal int64) int64 {
// 	val := os.Getenv(key)
// 	if val == "" {
// 		return defaultVal
// 	}

// 	var intVal, err = strconv.Atoi(val)
// 	if err != nil {
// 		fmt.Println("Convert env string val to int error")
// 		return defaultVal
// 	}
// 	return int64(intVal)
// }

func getIntEnvDefault(key string, defaultVal int) int {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}

	var intVal, err = strconv.Atoi(val)
	if err != nil {
		fmt.Println("Convert env string val to int error")
		return defaultVal
	}
	return intVal
}

func getBoolEnvDefault(key string, defaultVal bool) bool {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}

	var boolVal, err = strconv.ParseBool(val)
	if err != nil {
		fmt.Println("Convert env string val to bool error")
		return defaultVal
	}
	return boolVal
}
