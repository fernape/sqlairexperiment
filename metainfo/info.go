package metainfo

import (
	"reflect"
	"strings"

	"github.com/pkg/errors"
)

// Field represents a single field from a struct type.
type Field struct {
	value reflect.Value

	// Name is the name of the struct field.
	Name string

	// Index of this field in the structure
	Index int

	// OmitEmpty is true when "omitempty" is
	// a property of the field's "db" tag.
	OmitEmpty bool
}

// TypeInfo represents reflected information about a struct type.
type TypeInfo struct {
	value reflect.Value

	// Fields maps "db" tags to struct fields.
	// Sqlair does not care about fields without a "db" tag.
	Fields map[string]Field
	// Tags maps field names to tags
	Tags map[string]string
}

// Kind returns the TypeInfo's reflect.Kind.
func (r TypeInfo) Kind() reflect.Kind {
	return r.value.Kind()
}

// Name returns the name of the TypeInfo's type.
func (r TypeInfo) Name() string {
	return r.value.Type().Name()
}

// generate produces and returns reflection information for the input
// reflect.Value that is specifically required for Sqlair operation.
func Generate(v any) (TypeInfo, error) {
	value := reflect.ValueOf(v)
	// Dereference the pointer if it is one.
	value = reflect.Indirect(value)

	// If this is a not a struct, we can not provide
	// any further reflection information.
	// FIXME: We need to support &M derived from map[string]any
	if value.Kind() != reflect.Struct {
		return TypeInfo{value: value}, nil
	}

	info := TypeInfo{
		Fields: make(map[string]Field),
		Tags:   make(map[string]string),
		value:  value,
	}

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
			return TypeInfo{}, err
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
