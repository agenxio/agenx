package scribe

import (
	"github.com/queueio/sentry/utils/types/event"
	"github.com/queueio/sentry/utils/types/maps"
)

type Data struct {
	Event event.Event
	state State
}

func NewData() *Data {
	return &Data{}
}

// SetState sets the state
func (d *Data) SetState(state State) {
	d.state = state
}

// GetState returns the current state
func (d *Data) GetState() State {
	return d.state
}

// HasState returns true if the data object contains state data
func (d *Data) HasState() bool {
	return d.state != State{}
}

// GetEvent returns the event in the data object
// In case meta data contains module and fileset data, the event is enriched with it
func (d *Data) GetEvent() event.Event {
	return d.Event
}

// GetMetadata creates a common.MapStr containing the metadata to
// be associated with the event.
func (d *Data) GetMetadata() maps.StringIf {
	return d.Event.Meta
}

// HasEvent returns true if the data object contains event data
func (d *Data) HasEvent() bool {
	return d.Event.Fields != nil
}
