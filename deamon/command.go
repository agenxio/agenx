package scribe

import (
	"github.com/queueio/sentry/utils/command"
)

// Name of this SENTRY
var Name = "scribe"

// Root command to handle SENTRY client
var Command = command.Root(Name, "", New)
