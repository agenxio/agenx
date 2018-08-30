package event

import (
	"errors"
	"time"
	"github.com/agenxio/agenx/common/types/maps"
)

type Event struct {
	Topic     string
	Timestamp time.Time
	Meta      maps.StringIf
	Fields    maps.StringIf
	Private   interface{}
}

var (
	errNoTimestamp = errors.New("value is no timestamp")
)

func (e *Event) GetValue(key string) (interface{}, error) {
	if key == "@timestamp" {
		return e.Timestamp, nil
	}
	return e.Fields.GetValue(key)
}

func (e *Event) PutValue(key string, v interface{}) (interface{}, error) {
	if key == "@timestamp" {
		switch ts := v.(type) {
		case time.Time:
			e.Timestamp = ts
			/*
		case common.Time:
			e.Timestamp = time.Time(ts)
			*/
		default:
			return nil, errNoTimestamp
		}
	}

	// TODO: add support to write into '@metadata'?
	return e.Fields.Put(key, v)
}

func (e *Event) Delete(key string) error {
	return e.Fields.Delete(key)
}
