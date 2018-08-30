package command

import (
	"fmt"
	"os"
	"runtime"

	"github.com/spf13/cobra"

	"github.com/queueio/sentry/utils/version"
	"github.com/queueio/sentry/utils/command/instance"
)

func Version(name, ver string) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Show current version info",
		Run: func(cmd *cobra.Command, args []string) {
			instance, err := instance.New(name, ver)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error initializing sentry: %s\n", err)
				os.Exit(1)
			}

			fmt.Printf("%s version %s (%s), utils %s\n",
						instance.Info.Component, instance.Info.Version, runtime.GOARCH,
						version.GetDefaultVersion())
		},
	}
}
