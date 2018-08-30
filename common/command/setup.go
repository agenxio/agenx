package command

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/queueio/sentry/utils/component"
	"github.com/queueio/sentry/utils/command/instance"
)

func Setup(name, version string, factory component.Factory) *cobra.Command {
	setup := cobra.Command{
		Use:   "setup",
		Short: "Setup plugins",

		Run: func(cmd *cobra.Command, args []string) {
			component, err := instance.New(name, version)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error initializing sentry: %s\n", err)
				os.Exit(1)
			}

			if err = component.Setup(factory); err != nil {
				os.Exit(1)
			}
		},
	}

	return &setup
}
