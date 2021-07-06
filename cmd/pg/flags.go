package pg

import (
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// deleteCmd represents the delete command
var flagsCmd = &cobra.Command{
	Use:   "flags",
	Short: "list all global flags",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Usage()
	},
}

func init() {
	cmd.AddCommand(flagsCmd)

	defaultUsageFn := (&cobra.Command{}).UsageFunc()
	defaultHelpFn := (&cobra.Command{}).HelpFunc()

	flagsCmd.Short = `Display the list of all available global flags`

	flagsCmd.DisableFlagsInUseLine = true
	flagsCmd.SetUsageTemplate(flagsUsageTemplate)
	flagsCmd.SetHelpTemplate(flagsHelpTemplate)

	flagsCmd.SetUsageFunc(func(cmd *cobra.Command) error {
		cmd.Parent().PersistentFlags().VisitAll(func(f *pflag.Flag) { f.Hidden = false })

		return defaultUsageFn(cmd)
	})
	flagsCmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		cmd.Parent().PersistentFlags().VisitAll(func(f *pflag.Flag) { f.Hidden = false })

		defaultHelpFn(cmd, args)
	})
}

const flagsHelpTemplate = `{{with (or .Long .Short)}}{{. | trimTrailingWhitespaces}}{{end}}

Usage:
{{.UseLine}}

Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}

{{.Usage}}`
const flagsUsageTemplate = `Global Flags:
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}
`
