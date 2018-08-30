package reader

import (
	"fmt"
	"time"
	"bytes"
	"encoding/json"

	"github.com/queueio/sentry/utils/log"
	"github.com/queueio/sentry/utils/encoding"
	"github.com/queueio/sentry/utils/types/maps"
	"github.com/queueio/sentry/utils/types/event"
)

type JSON struct {
	reader Reader
	cfg    *JSONConfig
}

func NewJSON(r Reader, cfg *JSONConfig) *JSON {
	return &JSON{reader: r, cfg: cfg}
}

func (r *JSON) decodeJSON(text []byte) ([]byte, maps.StringIf) {
	var jsonFields map[string]interface{}

	err := unmarshal(text, &jsonFields)
	if err != nil || jsonFields == nil {
		log.Err("Error decoding JSON: %v", err)
		if r.cfg.AddErrorKey {
			jsonFields = maps.StringIf{"error": createJSONError(fmt.Sprintf("Error decoding JSON: %v", err))}
		}
		return text, jsonFields
	}

	if len(r.cfg.MessageKey) == 0 {
		return []byte(""), jsonFields
	}

	textValue, ok := jsonFields[r.cfg.MessageKey]
	if !ok {
		if r.cfg.AddErrorKey {
			jsonFields["error"] = createJSONError(fmt.Sprintf("Key '%s' not found", r.cfg.MessageKey))
		}
		return []byte(""), jsonFields
	}

	textString, ok := textValue.(string)
	if !ok {
		if r.cfg.AddErrorKey {
			jsonFields["error"] = createJSONError(fmt.Sprintf("Value of key '%s' is not a string", r.cfg.MessageKey))
		}
		return []byte(""), jsonFields
	}

	return []byte(textString), jsonFields
}

func unmarshal(text []byte, fields *map[string]interface{}) error {
	dec := json.NewDecoder(bytes.NewReader(text))
	dec.UseNumber()
	err := dec.Decode(fields)
	if err != nil {
		return err
	}

	encoding.TransformNumbers(*fields)
	return nil
}

func (r *JSON) Next() (Message, error) {
	message, err := r.reader.Next()
	if err != nil {
		return message, err
	}

	var fields maps.StringIf
	message.Content, fields = r.decodeJSON(message.Content)
	message.AddFields(maps.StringIf{"json": fields})
	return message, nil
}

func createJSONError(message string) maps.StringIf {
	return maps.StringIf{"message": message, "type": "json"}
}

func MergeJSONFields(data maps.StringIf, jsonFields maps.StringIf, text *string, config JSONConfig) {
	if len(config.MessageKey) > 0 && text != nil {
		jsonFields[config.MessageKey] = *text
	}

	if config.KeysUnderRoot {
		delete(data, "json")

		var ts time.Time
		if v, ok := data["@timestamp"]; ok {
			switch t := v.(type) {
			case time.Time:
				ts = t
				/*
			case common.Time:
				ts = time.Time(ts)
				*/
			}
		}
		event := &event.Event{
			Timestamp: ts,
			Fields:    data,
		}
		encoding.WriteJSONKeys(event, jsonFields, config.OverwriteKeys)

		/*
		if !event.Timestamp.IsZero() {
			data["@timestamp"] = common.Time(event.Timestamp)
		}
		*/
	}
}