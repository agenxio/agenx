package scribe

import (
	"time"
	"github.com/queueio/sentry/utils/config"
)

type Config struct {
	*Scribe

	Topic      string
	Pipeline   string
	Flight     int
	Publishers int
	Sample     float64
	Region    *regionConfig
	Registry  *registryConfig

	Inputs    []*config.Config
}

type regionConfig struct {
	Cluster []string
}

type registryConfig struct {
	File   string
	Flush  time.Duration
}

var (
	DefaultConfig = Config{}
)

var (
	defaultConfig = InputConfig{
		Scan: Scan{
			Frequency: 10 * time.Second,
		},

		Type: "log",
	}
)

type InputConfig struct {
	Scan  Scan
	Type  string
}

type Scan struct {
	Frequency time.Duration
}