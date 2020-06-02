package mysql

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

var ShortDescription = "MySQL backup tool"

// These variables are here only to show current version. They are set in makefile during build process
var WalgVersion = "devel"
var GitRevision = "devel"
var BuildDate = "devel"

var Cmd = &cobra.Command{
	Use:     "mysql",
	Short:   ShortDescription, // TODO : improve description
	Version: strings.Join([]string{WalgVersion, GitRevision, BuildDate, "MySQL"}, "\t"),
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		err := internal.AssertRequiredSettingsSet()
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func Execute() {
	if err := Cmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(internal.InitConfig, internal.Configure)

	internal.RequiredSettings[internal.MysqlDatasourceNameSetting] = true
	Cmd.PersistentFlags().StringVar(&internal.CfgFile, "config", "", "config file (default is $HOME/.walg.json)")
	Cmd.InitDefaultVersionFlag()
	internal.AddConfigFlags(Cmd)
}
