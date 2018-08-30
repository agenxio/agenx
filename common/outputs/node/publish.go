package node

import "github.com/queueio/sentry/utils/types/event"

type publisher struct {
	channel  *chan event.Event
	index    int
}

func (p *publisher) Publish(e event.Event) error {
	*p.channel <- e
	return nil
}
