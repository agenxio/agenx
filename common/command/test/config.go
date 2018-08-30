package test

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/queueio/sentry/utils/component"
	"github.com/queueio/sentry/utils/command/instance"
)

func TestConfigCommand(name, version string, f component.Factory) *cobra.Command {
	command := cobra.Command{
		Use:   "config",
		Short: "Test configuration settings",
		Run: func(cmd *cobra.Command, args []string) {
			component, err := instance.New(name, version)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error initializing sentry: %s\n", err)
				os.Exit(1)
			}

			if err = component.TestConfig(f); err != nil {
				os.Exit(1)
			}
		},
	}

	return &command
}
