/*
Copyright National Payments Corporation of India. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"os"
	"strings"

	"github.com/npci/drunix/internal/peer/common"
	"github.com/npci/drunix/internal/peer/version"
	"github.com/npci/drunix/internal/vscc/node"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// The main command describes the service and
// defaults to printing the help message.
var mainCmd = &cobra.Command{Use: "vscc"}

var loggingLevel string = "logging-level"

func main() {

	// For environment variables.
	viper.SetEnvPrefix(common.CmdRoot)
	viper.AutomaticEnv()
	replacer := strings.NewReplacer(".", "_")
	viper.SetEnvKeyReplacer(replacer)

	// Define command-line flags that are valid for all peer commands and
	// subcommands.
	mainFlags := mainCmd.PersistentFlags()

	mainFlags.String(loggingLevel, "", "Legacy logging level flag")
	viper.BindPFlag("logging_level", mainFlags.Lookup(loggingLevel))
	mainFlags.MarkHidden(loggingLevel)

	mainCmd.AddCommand(version.Cmd())
	mainCmd.AddCommand(node.Cmd())
	// On failure Cobra prints the usage message and error string, so we only
	// need to exit with a non-0 status
	if mainCmd.Execute() != nil {
		os.Exit(1)
	}
}
