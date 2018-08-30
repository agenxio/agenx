package deamon

import (
	"github.com/queueio/sentry/utils/config"
	"github.com/queueio/sentry/utils/queue"
)

type runner struct {
	handler     queue.Handler
	registrar    *Registrar
	sentryDone  chan struct{}
}

func newRunner(handler queue.Handler, registrar *Registrar, sentryDone chan struct{}) *runner {
	return &runner{
		handler:    handler,
		registrar:    registrar,
		sentryDone: sentryDone,
	}
}

func (r *runner) Create(c *config.Config) (Runner, error) {
	robot, err := NewRobot(c, r.handler, r.sentryDone, r.registrar.GetStates())
	if err != nil {
		return robot, err
	}

	return robot, nil
}
