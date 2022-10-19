package typeinfo

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

var cmutex sync.RWMutex
var cache = make(map[reflect.Type]Info)

// Reflect will return the Info of a given type,
// generating and caching as required.
func GetTypeInfo(value any) (Info, error) {
	if value == (any)(nil) {
		return Info{}, fmt.Errorf("Can not reflect nil value")
	}

	v := reflect.ValueOf(value)

	v = reflect.Indirect(v)

	cmutex.RLock()
	info, found := cache[v.Type()]
	cmutex.RUnlock()
	if found {
		return info, nil
	}

	ri, err := generate(v)
	if err != nil {
		return Info{}, err
	}

	cmutex.Lock()
	cache[v.Type()] = ri
	cmutex.Unlock()
	return ri, nil
}

// generate produces and returns reflection information for the input
// reflect.Value that is specifically required for Sqlair operation.
func generate(value reflect.Value) (Info, error) {
	// Dereference the pointer if it is one.
	value = reflect.Indirect(value)

	// If this is a not a struct, we can not provide
	// any further reflection information.
	if value.Kind() != reflect.Struct {
		if value.Kind() != reflect.Map && reflect.TypeOf(value).Name() != "sqlair.M" {
			return Info{value: value}, nil
		}
	}

	info := Info{
		Fields: make(map[string]Field),
		Tags:   make(map[string]string),
		value:  value,
	}

	switch value.Kind() {
	case reflect.Struct:
		typ := value.Type()
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			// Fields without a "db" tag are outside of Sqlair's remit.
			tag := field.Tag.Get("db")
			if tag == "" {
				continue
			}

			tag, omitEmpty, err := parseTag(tag)
			if err != nil {
				return Info{}, err
			}

			info.Fields[tag] = Field{
				Name:      field.Name,
				Index:     i,
				OmitEmpty: omitEmpty,
				value:     value.Field(i),
			}
			info.Tags[field.Name] = tag
		}
		return info, nil
	case reflect.Map:
		for _, key := range value.MapKeys() {
			info.Fields[key.String()] = Field{
				Name:      key.String(),
				OmitEmpty: false,
				value:     value.MapIndex(key),
			}
		}
		return info, nil

	}
	return info, nil
}

// parseTag parses the input tag string and returns its
// name and whether it contains the "omitempty" option.
func parseTag(tag string) (string, bool, error) {
	options := strings.Split(tag, ",")

	var omitEmpty bool
	if len(options) > 1 {
		if strings.ToLower(options[1]) != "omitempty" {
			return "", false, errors.Errorf("unexpected tag value %q", options[1])
		}
		omitEmpty = true
	}

	return options[0], omitEmpty, nil
}
