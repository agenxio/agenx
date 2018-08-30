package scribe

import (
	"fmt"

	"github.com/queueio/sentry/utils/log"
	"github.com/queueio/sentry/utils/config"
	"github.com/queueio/sentry/utils/version"
	"github.com/queueio/sentry/utils/component"
)

type Scribe struct {
	config  *Config
	done     chan struct{}
}

func New(sentry *component.Sentry, raw *config.Config) (component.Component, error) {
	config := &DefaultConfig
	if err := raw.Unpack(config); err != nil {
		log.Err("Error reading config file: %v", err)
		return nil, err
	}

	scribe := &Scribe{
		config: config,
		done: make(chan struct{}),
	}

	log.Info("%s %s", sentry.Info.Name, version.GetDefaultVersion())
	return scribe, nil
}

func (s *Scribe) Run(sentry *component.Sentry) error {
	var err error
	config := s.config

	waitFinished := newSignalWait()
	waitEvents := newSignalWait()

	registrar, err := newRegistrar(config.Registry.File, config.Registry.Flush, nil)
	if err != nil {
		log.Err("Could not init registrar: %v", err)
		return err
	}

	input, err := newInput(sentry.Info, sentry.Handler, s.config.Inputs, s.done)
	if err != nil {
		return err
	}

	err = registrar.Start()
	if err != nil {
		return fmt.Errorf("Could not start registrar: %v", err)
	}
	defer registrar.Stop()

	defer waitEvents.Wait()

	err = input.start(registrar, config)
	if err != nil {
		input.stop()
		return err
	}

	waitFinished.AddChan(s.done)
	waitFinished.Wait()

	input.stop()
	return nil
}

func (s *Scribe) Stop() {
	log.Info("Stopping scribe")
	// Stop Scribe
	close(s.done)
}
