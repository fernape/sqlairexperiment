package typeinfo

import (
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

type M map[string]any
