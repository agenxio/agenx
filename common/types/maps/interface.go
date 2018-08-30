package maps

import (
	"encoding/json"
	"fmt"
	"strings"
	"github.com/pkg/errors"
)

const (
	FieldsKey = "fields"
	TagsKey   = "tags"
)

var (
	ErrKeyNotFound = errors.New("key not found")
)

type EventMetadata struct {
	Fields          StringIf
	FieldsUnderRoot bool `config:"fields_under_root"`
	Tags            []string
}

type StringIf map[string]interface{}

func (m StringIf) Update(d StringIf) {
	for k, v := range d {
		m[k] = v
	}
}

func (m StringIf) DeepUpdate(d StringIf) {
	for k, v := range d {
		switch val := v.(type) {
		case map[string]interface{}:
			m[k] = deepUpdateValue(m[k], StringIf(val))
		case StringIf:
			m[k] = deepUpdateValue(m[k], val)
		default:
			m[k] = v
		}
	}
}

func deepUpdateValue(old interface{}, val StringIf) interface{} {
	if old == nil {
		return val
	}

	switch sub := old.(type) {
	case StringIf:
		sub.DeepUpdate(val)
		return sub
	case map[string]interface{}:
		tmp := StringIf(sub)
		tmp.DeepUpdate(val)
		return tmp
	default:
		return val
	}
}

func (m StringIf) Delete(key string) error {
	_, err := walkStringIf(key, m, opDelete)
	return err
}

func (m StringIf) CopyFieldsTo(to StringIf, key string) error {
	v, err := walkStringIf(key, m, opGet)
	if err != nil {
		return err
	}

	_, err = walkStringIf(key, to, mapStrOperation{putOperation{v}, true})
	return err
}

func (m StringIf) Clone() StringIf {
	result := StringIf{}

	for k, v := range m {
		if innerStringIf, ok := tryToStringIf(v); ok {
			v = innerStringIf.Clone()
		}
		result[k] = v
	}

	return result
}

func (m StringIf) HasKey(key string) (bool, error) {
	hasKey, err := walkStringIf(key, m, opHasKey)
	if err != nil {
		return false, err
	}

	return hasKey.(bool), nil
}

func (m StringIf) GetValue(key string) (interface{}, error) {
	return walkStringIf(key, m, opGet)
}

func (m StringIf) Put(key string, value interface{}) (interface{}, error) {
	return walkStringIf(key, m, mapStrOperation{putOperation{value}, true})
}

func (m StringIf) StringToPrint() string {
	json, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Sprintf("Not valid json: %v", err)
	}
	return string(json)
}

func (m StringIf) String() string {
	bytes, err := json.Marshal(m)
	if err != nil {
		return fmt.Sprintf("Not valid json: %v", err)
	}
	return string(bytes)
}

func (m StringIf) Flatten() StringIf {
	return flatten("", m, StringIf{})
}

func flatten(prefix string, in, out StringIf) StringIf {
	for k, v := range in {
		var fullKey string
		if prefix == "" {
			fullKey = k
		} else {
			fullKey = fmt.Sprintf("%s.%s", prefix, k)
		}

		if m, ok := tryToStringIf(v); ok {
			flatten(fullKey, m, out)
		} else {
			out[fullKey] = v
		}
	}
	return out
}

func StringIfUnion(dict1 StringIf, dict2 StringIf) StringIf {
	dict := StringIf{}

	for k, v := range dict1 {
		dict[k] = v
	}

	for k, v := range dict2 {
		dict[k] = v
	}
	return dict
}

func MergeFields(ms, fields StringIf, underRoot bool) error {
	if ms == nil || len(fields) == 0 {
		return nil
	}

	fieldsMS := ms
	if !underRoot {
		f, ok := ms[FieldsKey]
		if !ok {
			fieldsMS = make(StringIf, len(fields))
			ms[FieldsKey] = fieldsMS
		} else {
			// Use existing 'fields' value.
			var err error
			fieldsMS, err = toStringIf(f)
			if err != nil {
				return err
			}
		}
	}

	for k, v := range fields {
		fieldsMS[k] = v
	}

	return nil
}

func AddTags(ms StringIf, tags []string) error {
	if ms == nil || len(tags) == 0 {
		return nil
	}

	tagsIfc, ok := ms[TagsKey]
	if !ok {
		ms[TagsKey] = tags
		return nil
	}

	existingTags, ok := tagsIfc.([]string)
	if !ok {
		return errors.Errorf("expected string array by type is %T", tagsIfc)
	}

	ms[TagsKey] = append(existingTags, tags...)
	return nil
}

func toStringIf(v interface{}) (StringIf, error) {
	m, ok := tryToStringIf(v)
	if !ok {
		return nil, errors.Errorf("expected map but type is %T", v)
	}
	return m, nil
}

func tryToStringIf(v interface{}) (StringIf, bool) {
	switch m := v.(type) {
	case StringIf:
		return m, true
	case map[string]interface{}:
		return StringIf(m), true
	default:
		return nil, false
	}
}

func walkStringIf(key string, data StringIf, op mapStrOperation) (interface{}, error) {
	var err error
	keyParts := strings.Split(key, ".")

	m := data
	for i, k := range keyParts[0 : len(keyParts)-1] {
		v, exists := m[k]
		if !exists {
			if op.CreateMissingKeys {
				newStringIf := StringIf{}
				m[k] = newStringIf
				m = newStringIf
				continue
			}
			return nil, errors.Wrapf(ErrKeyNotFound, "key=%v", strings.Join(keyParts[0:i+1], "."))
		}

		m, err = toStringIf(v)
		if err != nil {
			return nil, errors.Wrapf(err, "key=%v", strings.Join(keyParts[0:i+1], "."))
		}
	}

	v, err := op.Do(keyParts[len(keyParts)-1], m)
	if err != nil {
		return nil, errors.Wrapf(err, "key=%v", key)
	}

	return v, nil
}

var (
	opDelete = mapStrOperation{deleteOperation{}, false}
	opGet    = mapStrOperation{getOperation{}, false}
	opHasKey = mapStrOperation{hasKeyOperation{}, false}
)

type mapStrOperation struct {
	mapStrOperator
	CreateMissingKeys bool
}

type mapStrOperator interface {
	Do(key string, data StringIf) (value interface{}, err error)
}

type deleteOperation struct{}

func (op deleteOperation) Do(key string, data StringIf) (interface{}, error) {
	value, found := data[key]
	if !found {
		return nil, ErrKeyNotFound
	}
	delete(data, key)
	return value, nil
}

type getOperation struct{}

func (op getOperation) Do(key string, data StringIf) (interface{}, error) {
	value, found := data[key]
	if !found {
		return nil, ErrKeyNotFound
	}
	return value, nil
}

type hasKeyOperation struct{}

func (op hasKeyOperation) Do(key string, data StringIf) (interface{}, error) {
	_, found := data[key]
	return found, nil
}

type putOperation struct {
	Value interface{}
}

func (op putOperation) Do(key string, data StringIf) (interface{}, error) {
	existingValue, _ := data[key]
	data[key] = op.Value
	return existingValue, nil
}