package reader

import (
	"time"

	"github.com/queueio/sentry/utils/types/maps"
)

type Message struct {
	Ts      time.Time     // timestamp the content was read
	Content []byte        // actual content read
	Bytes   int           // total number of bytes read to generate the message
	Fields  maps.StringIf // optional fields that can be added by reader
}

func (m *Message) IsEmpty() bool {
	if m.Bytes == 0 {
		return true
	}

	if len(m.Content) == 0 && len(m.Fields) == 0 {
		return true
	}

	return false
}

func (msg *Message) AddFields(fields maps.StringIf) {
	if fields == nil {
		return
	}

	if msg.Fields == nil {
		msg.Fields = maps.StringIf{}
	}
	msg.Fields.Update(fields)
}