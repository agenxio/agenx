package scribe

import (
	"fmt"

	"github.com/queueio/sentry/utils/log"
cfg	"github.com/queueio/sentry/utils/config"
	"github.com/queueio/sentry/utils/queue"
)

type Context struct {
	States     []State
	Done       chan struct{}
	SentryDone chan struct{}
}

type Factory func(config *cfg.Config, handler queue.Handler, context Context) (Collector, error)

var factories = make(map[string]Factory)

func Register(name string, factory Factory) error {
	log.Info("Register collector factory")
	if name == "" {
		return fmt.Errorf("Error register collector: name cannot be empty")
	}
	if factory == nil {
		return fmt.Errorf("Error register collector '%v': factory cannot be empty", name)
	}
	if _, exists := factories[name]; exists {
		return fmt.Errorf("Error register collector '%v': already registered", name)
	}

	factories[name] = factory
	log.Info("Successfully registered collector")

	return nil
}

func Load(name string) (Factory, error) {
	if _, exists := factories[name]; !exists {
		return nil, fmt.Errorf("Error retrieving factory for collector '%v'", name)
	}
	return factories[name], nil
}
