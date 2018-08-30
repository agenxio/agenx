package outputs

import (
	"github.com/queueio/sentry/utils/queue"
	"github.com/queueio/sentry/utils/types/event"
	"github.com/queueio/sentry/utils/types"
)

type Client interface {
	queue.Handler
	queue.FailedMessageLogger
	Close() error
}

type Publisher interface {
	Publish(e event.Event) error
}

type OutputAdapter interface {
	Group(name string) types.Object
}

func GroupPublish(name string, handler queue.Handler) Publisher {
    adapter := handler.(OutputAdapter)
    return adapter.Group(name).(Publisher)
}