package scribe

import (
	"os"
	"fmt"
	"syscall"
	"sync"
	"os/signal"
	"encoding/json"

	"github.com/mitchellh/hashstructure"

	"github.com/queueio/sentry/utils/log"
	"github.com/queueio/sentry/utils/queue"
	"github.com/queueio/sentry/utils/config"
	"github.com/queueio/sentry/utils/version"
	"github.com/queueio/sentry/utils/component"
)

var reloadDebug = log.MakeDebug("reload")

type RunnerFactory interface {
	Create(config *config.Config) (Runner, error)
}

type Runner interface {
	Start()
	Stop()
}

type Reload struct {
	module    *Module
	config    *Config
	done       chan struct{}
	wg         sync.WaitGroup
	runner     RunnerFactory
	configs []*config.Config

	qConfig   *queue.Config
	consumer  *queue.Consumer
}

func newReload(config *Config, info component.Info) (*Reload, error) {
	var err       error
	var qConfig  *queue.Config

	reload := &Reload{
				module: NewModule(),
				config: config,
				done: make(chan struct{}),
	}

	if config.Topic == "" {
		log.Critical("topic are required")
		os.Exit(0)
	}

	if config.Pipeline == "" {
		config.Pipeline = info.Hostname
		log.Info("pipeline name is: %s", config.Pipeline)
	}

	if config.Sample > 1.0 || config.Sample < 0.0 {
		log.Critical("ERROR: --sample must be between 0.0 and 1.0")
	}

	qConfig = queue.NewConfig()
	qConfig.UserAgent = fmt.Sprintf("scribe/%s queue/%s", version.GetDefaultVersion(), queue.VERSION)
	qConfig.MaxInFlight = config.Flight
	reload.consumer, err = queue.NewConsumer(config.Topic, config.Pipeline, qConfig)
	if err != nil {
		log.Critical(err.Error())
	}
	reload.qConfig = qConfig

	return reload, nil
}

func (r *Reload) Run(runner RunnerFactory) {
	r.runner = runner
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	r.consumer.AddConcurrentHandlers(r, r.config.Publishers)
	err := r.consumer.ConnectToRegions(r.config.Region.Cluster)
	if err != nil {
		log.Critical(err.Error())
		os.Exit(0)
	}

	for {
		select {
		case <-r.consumer.StopChan:
			log.Warn("consumer stop")
		case <-termChan:
			r.Stop()
		}
	}
}

func (r *Reload) Stop() {
	close(r.done)
	r.wg.Wait()
}

// drive scribe running by queue message
func (r *Reload) HandleMessage(message *queue.Message) error {
	var v struct{
		Pipeline  string
		Config    []byte
	}
	if err := json.Unmarshal(message.Body, &v); err != nil {
		return err
	}

	if v.Pipeline != r.config.Pipeline {
		return nil
	}

	return reloadModules(r, v.Config)
}

func reloadModules(r *Reload, stream []byte) error {
	var err error
	config, err := config.LoadStream(stream)
	if err != nil {
		return err
	}

	if err := config.Unpack(r.configs); err != nil {
		log.Err("config unpack error: %s", err.Error())
		return err
	}

	reloadDebug("Number of module configs found: %v", len(r.configs))

	startList := map[uint64]Runner{}
	stopList := r.module.CopyList()

	for _, c := range r.configs {

		if !c.Enabled() {
			continue
		}

		rawCfg := map[string]interface{}{}
		err := c.Unpack(rawCfg)

		if err != nil {
			log.Err("Unable to unpack config file due to error: %v", err)
			continue
		}

		hash, err := hashstructure.Hash(rawCfg, nil)
		if err != nil {
			reloadDebug("Unable to generate hash for config file %v due to error: %v", c, err)
			continue
		}

		reloadDebug("Remove module from stoplist: %v", hash)
		delete(stopList, hash)

		// As module already exist, it must be removed from the stop list and not started
		if !r.module.Has(hash) {
			reloadDebug("Add module to startlist: %v", hash)
			runner, err := r.runner.Create(c)
			if err != nil {
				log.Err("Unable to create runner due to error: %v", err)
				continue
			}
			startList[hash] = runner
		}
	}

	r.stopRunners(stopList)
	r.startRunners(startList)
	return nil
}

// delivery 10 failure message
func (r *Reload) LogFailedMessage(message *queue.Message) {
	log.Err("%v", message.Body)
}

func (r *Reload) startRunners(list map[uint64]Runner) {
	if len(list) == 0 {
		return
	}

	log.Info("Starting %v runners ...", len(list))
	for id, runner := range list {
		runner.Start()
		r.module.Add(id, runner)
		reloadDebug("New runner started: %v", id)
	}
}

func (r *Reload) stopRunners(list map[uint64]Runner) {
	if len(list) == 0 {
		return
	}

	log.Info("Stopping %v runners ...", len(list))

	wg := sync.WaitGroup{}
	for hash, runner := range list {
		wg.Add(1)

		go func(h uint64, run Runner) {
			defer func() {
				reloadDebug("Runner stopped: %v", h)
				wg.Done()
			}()

			run.Stop()
			r.module.Remove(h)
		}(hash, runner)
	}

	wg.Wait()
}
