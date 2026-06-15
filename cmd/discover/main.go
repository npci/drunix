/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"os"

	"github.com/npci/drunix/bccsp/factory"
	"github.com/npci/drunix/cmd/common"
	discovery "github.com/npci/drunix/discovery/cmd"
)

func main() {
	factory.InitFactories(nil)
	cli := common.NewCLI("discover", "Command line client for fabric discovery service")
	discovery.AddCommands(cli)
	cli.Run(os.Args[1:])
}
