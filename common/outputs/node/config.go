package node

import "time"

type Config struct {
	Timeout timeoutConfig
	Publish publishConfig

	URLs []string `config:urls`
}

type timeoutConfig struct {
	Connect time.Duration
	Request time.Duration
}

type publishConfig struct {
	Group int
	Size  int
	Timer time.Duration
}

var defaultConfig = Config{}