package main

import (
	"os"

	"github.com/agenxio/agenx/deamon"
// _	"github.com/queueio/sentry/components/scribe/log"
)

func main() {
	if err := deamon.Command.Execute(); err != nil {
		os.Exit(1)
	}
}
