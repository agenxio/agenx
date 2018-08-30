package deamon

import (
	"fmt"
	"os"
	"sync"
	"time"
	"path/filepath"
	"encoding/json"

	"github.com/queueio/sentry/utils/paths"
	"github.com/queueio/sentry/utils/log"
)

type Registrar struct {
	Channel      chan []State
	out          successLogger
	done         chan struct{}
	registrarFile  string // Path to the Registrar File
	wg           sync.WaitGroup

	states               *States // Map with all file paths inside and the corresponding state
	flushTimeout         time.Duration
	bufferedStateUpdates int
}

type successLogger interface {
	Published(n int) bool
}

func newRegistrar(registrarFile string, flushTimeout time.Duration, out successLogger) (*Registrar, error) {
	r := &Registrar{
		registrarFile:  registrarFile,
		done:         make(chan struct{}),
		states:       NewStates(),
		Channel:      make(chan []State, 1),
		flushTimeout: flushTimeout,
		out:          out,
		wg:           sync.WaitGroup{},
	}
	err := r.Init()

	return r, err
}

// Init sets up the Registrar and make sure the registrar file is setup correctly
func (r *Registrar) Init() error {
	// The registrar file is opened in the data path
	r.registrarFile = paths.Resolve(paths.Data, r.registrarFile)

	// Create directory if it does not already exist.
	registrarPath := filepath.Dir(r.registrarFile)
	err := os.MkdirAll(registrarPath, 0750)
	if err != nil {
		return fmt.Errorf("Failed to created registrar file dir %s: %v", registrarPath, err)
	}

	fileInfo, err := os.Lstat(r.registrarFile)
	if os.IsNotExist(err) {
		log.Info("No registrar file found under: %s. Creating a new registrar file.", r.registrarFile)
		return r.writeRegistrar()
	}
	if err != nil {
		return err
	}

	if !fileInfo.Mode().IsRegular() {
		if fileInfo.IsDir() {
			return fmt.Errorf("Registrar file path must be a file. %s is a directory.", r.registrarFile)
		}
		return fmt.Errorf("Registrar file path is not a regular file: %s", r.registrarFile)
	}

	log.Info("Registrar file set to: %s", r.registrarFile)
	return nil
}

func (r *Registrar) GetStates() []State {
	return r.states.GetStates()
}

func (r *Registrar) loadStates() error {
	f, err := os.Open(r.registrarFile)
	if err != nil {
		return err
	}

	defer f.Close()

	log.Info("Loading registrar data from %s", r.registrarFile)

	decoder := json.NewDecoder(f)
	states := []State{}
	err = decoder.Decode(&states)
	if err != nil {
		return fmt.Errorf("Error decoding states: %s", err)
	}

	states = resetStates(states)
	r.states.SetStates(states)
	log.Info("States Loaded from registrar: %+v", len(states))

	return nil
}

func resetStates(states []State) []State {
	for key, state := range states {
		state.Finished = true
		// Set ttl to -2 to easily spot which states are not managed by a prospector
		state.TTL = -2
		states[key] = state
	}
	return states
}

func (r *Registrar) Start() error {
	err := r.loadStates()
	if err != nil {
		return fmt.Errorf("Error loading state: %v", err)
	}

	r.wg.Add(1)
	go r.Run()

	return nil
}

func (r *Registrar) Run() {
	log.Info("Starting Registrar")
	defer func() {
		r.writeRegistrar()
		r.wg.Done()
	}()

	var (
		timer  *time.Timer
		flushC <-chan time.Time
	)

	for {
		select {
		case <-r.done:
			log.Info("Ending Registrar")
			return
		case <-flushC:
			flushC = nil
			timer.Stop()
			r.flushRegistrar()
		case states := <-r.Channel:
			r.onEvents(states)
			if r.flushTimeout <= 0 {
				r.flushRegistrar()
			} else if flushC == nil {
				timer = time.NewTimer(r.flushTimeout)
				flushC = timer.C
			}
		}
	}
}

func (r *Registrar) onEvents(states []State) {
	r.processEventStates(states)

	beforeCount := r.states.Count()
	cleanedStates := r.states.Cleanup()

	r.bufferedStateUpdates += len(states)

	log.Debug("registrar",
		"Registrar states cleaned up. Before: %d, After: %d",
		beforeCount, beforeCount-cleanedStates)
}

func (r *Registrar) processEventStates(states []State) {
	log.Debug("registrar", "Processing %d events", len(states))

	for i := range states {
		r.states.Update(states[i])
	}
}

func (r *Registrar) Stop() {
	log.Info("Stopping Registrar")
	close(r.done)
	r.wg.Wait()
}

func (r *Registrar) flushRegistrar() {
	if err := r.writeRegistrar(); err != nil {
		log.Err("Writing of registrar returned error: %v. Continuing...", err)
	}

	if r.out != nil {
		r.out.Published(r.bufferedStateUpdates)
	}
	r.bufferedStateUpdates = 0
}

func (r *Registrar) writeRegistrar() error {
	log.Debug("registrar", "Write registrar file: %s", r.registrarFile)

	tempfile := r.registrarFile + ".new"
	f, err := os.OpenFile(tempfile, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_SYNC, 0600)
	if err != nil {
		log.Err("Failed to create tempfile (%s) for writing: %s", tempfile, err)
		return err
	}

	states := r.states.GetStates()

	encoder := json.NewEncoder(f)
	err = encoder.Encode(states)
	if err != nil {
		f.Close()
		log.Err("Error when encoding the states: %s", err)
		return err
	}

	f.Close()

	err = SafeFileRotate(r.registrarFile, tempfile)

	log.Debug("registrar", "Registrar file updated. %d states written.", len(states))

	return err
}

func SafeFileRotate(path, tempfile string) error {
	if e := os.Rename(tempfile, path); e != nil {
		log.Err("Rotate error: %s", e)
		return e
	}
	return nil
}
