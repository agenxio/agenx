package deamon

import (
	"fmt"
	"sync"

	"github.com/queueio/sentry/utils/log"
	"github.com/queueio/sentry/utils/config"

	"github.com/queueio/sentry/utils/queue"
	"github.com/queueio/sentry/utils/component"
)

type Input struct {
	robots      map[uint64]*Robot
	configs     []*config.Config
	wg          sync.WaitGroup
	reload     *Reload
	done        chan struct{}
	sentryDone  chan struct{}
	output      queue.Handler
	info        component.Info
}

func newInput(
	info component.Info,
	output queue.Handler,
	inputs []*config.Config,
	done chan struct{}) (*Input, error) {
	return &Input{
		robots: map[uint64]*Robot{},
		configs:    inputs,
		done:       done,
		output:  output,
		info: info,
	}, nil
}

func (i *Input) start(registrar *Registrar, config *Config) error {
	log.Info("Loading inputs: %v", len(i.configs))

	for _, config := range i.configs {
		if !config.Enabled() {
			return nil
		}

		r, err := NewRobot(config, i.output, i.sentryDone, registrar.GetStates())
		if err != nil {
			return fmt.Errorf("Error in initing robot: %s", err)
		}

		if _, ok := i.robots[r.ID]; ok {
			return fmt.Errorf("Robot with same ID already exists: %d", r.ID)
		}

		i.robots[r.ID] = r

		r.Start()
	}

	var err error

	if i.reload, err = newReload(config, i.info); err != nil {
		return err
	}

	runner := newRunner(i.output, registrar, i.sentryDone)
	go func() {
		i.reload.Run(runner)
	}()

	return nil
}

func (i *Input) stop() error {
	for id, robot := range i.robots {
		robot.Stop()
		log.Info("%d number robot stop", id)
	}
	return nil
}
