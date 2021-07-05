package rocksdb

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

var ShortDescription = "Rocksdb backup tool"

// These variables are here only to show current version. They are set in makefile during build process
var walgVersion = "devel"
var gitRevision = "devel"
var buildDate = "devel"

var cmd = &cobra.Command{
	Use:     "rocksdb",
	Short:   ShortDescription, // TODO : improve description
	Version: strings.Join([]string{walgVersion, gitRevision, buildDate, "RocksDb"}, "\t"),
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		err := internal.AssertRequiredSettingsSet()
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func Execute() {
	if err := cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	internal.ConfigureSettings(internal.ROCKSDB)
	cobra.OnInitialize(internal.InitConfig, internal.Configure)

	cmd.PersistentFlags().StringVar(&internal.CfgFile, "config", "", "config file (default is $HOME/.walg.json)")
	cmd.PersistentFlags().BoolVarP(&internal.Turbo, "turbo", "", false, "Ignore all kinds of throttling defined in config")
	cmd.InitDefaultVersionFlag()
	internal.AddConfigFlags(cmd)
}
