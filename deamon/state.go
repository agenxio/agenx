package deamon

import (
	"os"
	"sync"
	"time"

	"github.com/queueio/sentry/utils/log"
)

type State struct {
	Id          string        `json:"-"` // local unique id to make comparison more efficient
	Finished    bool          `json:"-"` // harvester state
	Fileinfo    os.FileInfo   `json:"-"` // the file info
	Source      string        `json:"source"`
	Offset      int64         `json:"offset"`
	Timestamp   time.Time     `json:"timestamp"`
	TTL         time.Duration `json:"ttl"`
	Type        string        `json:"type"`
	FileStateOS StateOS
}

func NewState(fileInfo os.FileInfo, path string, t string) State {
	return State{
		Fileinfo:    fileInfo,
		Source:      path,
		Finished:    false,
		FileStateOS: GetOSState(fileInfo),
		Timestamp:   time.Now(),
		TTL:         -1, // By default, state does have an infinite ttl
		Type:        t,
	}
}

func (s *State) ID() string {
	if s.Id == "" {
		s.Id = s.FileStateOS.String()
	}
	return s.Id
}

func (s *State) IsEqual(c *State) bool {
	return s.ID() == c.ID()
}

func (s *State) IsEmpty() bool {
	return *s == State{}
}

type States struct {
	states []State
	sync.RWMutex
}

func NewStates() *States {
	return &States{
		states: []State{},
	}
}

func (s *States) Update(newState State) {
	s.Lock()
	defer s.Unlock()

	index, _ := s.findPrevious(newState)
	newState.Timestamp = time.Now()

	if index >= 0 {
		s.states[index] = newState
	} else {
		// No existing state found, add new one
		s.states = append(s.states, newState)
		log.Debug("state", "New state added for %s", newState.Source)
	}
}

func (s *States) FindPrevious(newState State) State {
	s.RLock()
	defer s.RUnlock()
	_, state := s.findPrevious(newState)
	return state
}

func (s *States) findPrevious(newState State) (int, State) {
	// TODO: This could be made potentially more performance by using an index (harvester id) and only use iteration as fall back
	for index, oldState := range s.states {
		// This is using the FileStateOS for comparison as FileInfo identifiers can only be fetched for existing files
		if oldState.IsEqual(&newState) {
			return index, oldState
		}
	}

	return -1, State{}
}

func (s *States) Cleanup() int {
	s.Lock()
	defer s.Unlock()

	statesBefore := len(s.states)

	currentTime := time.Now()
	states := s.states[:0]

	for _, state := range s.states {

		expired := (state.TTL > 0 && currentTime.Sub(state.Timestamp) > state.TTL)

		if state.TTL == 0 || expired {
			if state.Finished {
				log.Debug("state", "State removed for %v because of older: %v", state.Source, state.TTL)
				continue // drop state
			} else {
				log.Err("State for %s should have been dropped, but couldn't as state is not finished.", state.Source)
			}
		}

		states = append(states, state) // in-place copy old state
	}
	s.states = states

	return statesBefore - len(s.states)
}

func (s *States) Count() int {
	s.RLock()
	defer s.RUnlock()

	return len(s.states)
}

func (s *States) GetStates() []State {
	s.RLock()
	defer s.RUnlock()

	newStates := make([]State, len(s.states))
	copy(newStates, s.states)

	return newStates
}

func (s *States) SetStates(states []State) {
	s.Lock()
	defer s.Unlock()
	s.states = states
}

func (s *States) Copy() *States {
	states := NewStates()
	states.states = s.GetStates()
	return states
}
