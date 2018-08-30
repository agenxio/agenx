package deamon

import (
	"sync"
	"time"

	"github.com/mitchellh/hashstructure"

	"github.com/queueio/sentry/utils/log"
cfg	"github.com/queueio/sentry/utils/config"

	"github.com/queueio/sentry/utils/queue"
)

type Collector interface {
	Run()
	Stop()
}

type Robot struct {
	config     InputConfig
	collector  Collector
	done       chan struct{}
	wg        *sync.WaitGroup
	ID         uint64
	sentryDone chan struct{}
}

func NewRobot(conf *cfg.Config, handler queue.Handler, sentryDone chan struct{}, states []State) (*Robot, error) {
	robot := &Robot{
		config:     defaultConfig,
		wg:         &sync.WaitGroup{},
		done:       make(chan struct{}),
		sentryDone: sentryDone,
	}

	var err error
	if err = conf.Unpack(&robot.config); err != nil {
		return nil, err
	}

	var h map[string]interface{}
	if err := conf.Unpack(&h); err != nil {
		return nil, err
	}

	robot.ID, err = hashstructure.Hash(h, nil)
	if err != nil {
		return nil, err
	}

	var f Factory
	f, err = Load(robot.config.Type)
	if err != nil {
		return robot, err
	}

	context := Context{
		States:     states,
		Done:       robot.done,
		SentryDone: robot.sentryDone,
	}

	robot.collector, err = f(conf, handler, context)
	if err != nil {
		return robot, err
	}

	return robot, nil
}

func (r *Robot) Start() {
	r.wg.Add(1)
	log.Info("Starting robot of type: %v; ID: %d ", r.config.Type, r.ID)

	go func() {
		defer func() {
			r.stop()
			r.wg.Done()
		}()

		r.Run()
	}()
}

func (r *Robot) Run() {
	r.collector.Run()

	for {
		select {
		case <-r.done:
			log.Info("Robot ticker stopped")
			return
		case <-time.After(r.config.Scan.Frequency):
			log.Debug("robot", "Run collect robot")
			r.collector.Run()
		}
	}
}

func (r *Robot) Stop() {
	close(r.done)
	r.wg.Wait()
}

func (r *Robot) stop() {
	log.Info("Stopping Robot: %d", r.ID)
	r.collector.Stop()
}
