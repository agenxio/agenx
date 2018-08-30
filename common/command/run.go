package command

import (
	"flag"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/queueio/sentry/utils/component"
	"github.com/queueio/sentry/utils/command/instance"
)

func Run(name, version string, factory component.Factory, flags *pflag.FlagSet) *cobra.Command {
	run := cobra.Command{
		Use:   "run",
		Short: "Run " + name,
		Run: func(command *cobra.Command, args []string) {
			err := instance.Run(name, version, factory)
			if err != nil {
				os.Exit(1)
			}
		},
	}

	run.Flags().AddGoFlag(flag.CommandLine.Lookup("prof"))
	run.Flags().AddGoFlag(flag.CommandLine.Lookup("cpu"))
	run.Flags().AddGoFlag(flag.CommandLine.Lookup("mem"))
	run.Flags().AddGoFlag(flag.CommandLine.Lookup("N"))
	//run.Flags().AddGoFlag(flag.CommandLine.Lookup("setup"))
	run.Flags().AddGoFlag(flag.CommandLine.Lookup("test"))
	run.Flags().AddGoFlag(flag.CommandLine.Lookup("version"))

	//run.Flags().MarkDeprecated("version", "version flag has been deprecated, use version sub command")
	//run.Flags().MarkDeprecated("test", "config test flag has been deprecated, use test config sub command")

	if flags != nil {
		run.Flags().AddFlagSet(flags)
	}

	return &run
}
