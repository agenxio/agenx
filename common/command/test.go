package command

import (
	"github.com/spf13/cobra"

	"github.com/queueio/sentry/utils/component"
	"github.com/queueio/sentry/utils/command/test"
)

func Test(name, version string, f component.Factory) *cobra.Command {
	export := &cobra.Command{
		Use:   "test",
		Short: "Test configure",
	}

	export.AddCommand(test.TestConfigCommand(name, version, f))
	return export
}
