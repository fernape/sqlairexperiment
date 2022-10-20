package typeinfo

import (
	"fmt"
	"reflect"
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

// Struct represents reflected information about a struct type.
type Info struct {
	value reflect.Value

	// Fields maps "db" tags to struct fields.
	// Sqlair does not care about fields without a "db" tag.
	Fields map[string]Field
	// Tags maps field names to tags
	Tags map[string]string
}

// Kind returns the Info's reflect.Kind.
func (i Info) Kind() reflect.Kind {
	return i.value.Kind()
}

// Name returns the name of the Info's type.
func (i Info) Name() string {
	return i.value.Type().Name()
}

// GetType returns the reflect.Type of the value embedded in the data Info
// strcuture.
func (i Info) GetType() reflect.Type {
	return i.value.Type()
}

// GetFieldValue returns the real, concrete value for the fieldName passed as
// parameter if found and true indicating it was found.  If not found, returns
// an empty interface and false.
func GetFieldValue(st any, fieldName string) (any, error) {
	i, _ := GetTypeInfo(st)
	v, found := i.Fields[fieldName]
	if !found {
		return nil, fmt.Errorf("field %s not found", fieldName)
	}
	return reflect.ValueOf(st).Field(v.Index).Interface(), nil
}

// SetFieldValue sets the field corresponding to the tagName passed as
// paremeter, to the value "value" passed as parameter. Returns true if the
// value was set, false otherwise.
func SetFieldValue(st any, tagName string, value any) error {
	i, _ := GetTypeInfo(st)
	field, found := i.Fields[tagName]
	if !found {
		return fmt.Errorf("field %s not found", tagName)
	}

	if field.value.Type() != reflect.TypeOf(value) {
		return fmt.Errorf("type missmatch")
	}

	s := reflect.ValueOf(st).Elem()

	if !s.Field(field.Index).CanSet() {
		return fmt.Errorf("%s (%s) is not settable", field.Name, tagName)
	}

	s.Field(field.Index).Set(reflect.ValueOf(value))
	return nil
}

type M map[string]any
