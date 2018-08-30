package command

import (
	"flag"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/queueio/sentry/utils/component"
)

func init() {
	for i, arg := range os.Args[1:] {
		if strings.HasPrefix(arg, "-") && !strings.HasPrefix(arg, "--") && len(arg) > 2 {
			os.Args[1+i] = "-" + arg
		}
	}
}

type Command struct {
	cobra.Command

	Test     *cobra.Command
	Setup    *cobra.Command
	Run      *cobra.Command
	Version  *cobra.Command
}

func Root(name, version string, factory component.Factory) *Command {
	return RootWithFlags(name, version, factory, nil)
}

func RootWithFlags(name, version string, factory component.Factory, flags *pflag.FlagSet) *Command {
	command := &Command{}
	command.Use = name

	command.Run = Run(name, version, factory, flags)
	command.Setup = Setup(name, version, factory)
	command.Version = Version(name, version)
	command.Test = Test(name, version, factory)

	// Root command is an alias for run
	command.Command.Run = command.Run.Run

	// Persistent flags, common across all sub commands
	command.PersistentFlags().AddGoFlag(flag.CommandLine.Lookup("E"))
	command.PersistentFlags().AddGoFlag(flag.CommandLine.Lookup("c"))
	command.PersistentFlags().AddGoFlag(flag.CommandLine.Lookup("d"))
	command.PersistentFlags().AddGoFlag(flag.CommandLine.Lookup("v"))
	command.PersistentFlags().AddGoFlag(flag.CommandLine.Lookup("e"))
	command.PersistentFlags().AddGoFlag(flag.CommandLine.Lookup("path.config"))
	command.PersistentFlags().AddGoFlag(flag.CommandLine.Lookup("path.data"))
	command.PersistentFlags().AddGoFlag(flag.CommandLine.Lookup("path.logs"))
	command.PersistentFlags().AddGoFlag(flag.CommandLine.Lookup("path.home"))
	command.PersistentFlags().AddGoFlag(flag.CommandLine.Lookup("strict.perms"))
	if f := flag.CommandLine.Lookup("plugin"); f != nil {
		command.PersistentFlags().AddGoFlag(f)
	}

	// Inherit root flags from run command
	command.Flags().AddFlagSet(command.Run.Flags())

	// Register sub commands to all component
	command.AddCommand(command.Test)
	command.AddCommand(command.Setup)
	command.AddCommand(command.Run)
	command.AddCommand(command.Version)

	return command
}
