package outputs

import (
	"fmt"

	"github.com/queueio/sentry/utils/queue"
	"github.com/queueio/sentry/utils/config"
	"github.com/queueio/sentry/utils/component"
)

var output = map[string]Factory{}

type Factory func(info component.Info, config *config.Config) (queue.Handler, error)

func Register(name string, f Factory) {
	if output[name] != nil {
		panic(fmt.Errorf("output type '%v' exists already", name))
	}
	output[name] = f
}

func Find(name string) Factory {
	return output[name]
}

func Load(info component.Info, name string, config *config.Config) (queue.Handler, error) {
	factory := Find(name)
	if factory == nil {
		return nil, fmt.Errorf("output type %v undefined", name)
	}

	return factory(info, config)
}