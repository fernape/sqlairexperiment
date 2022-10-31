package typeinfo

import (
	"fmt"
	"reflect"
)

// Field represents a single field from a struct type.
type Field struct {
	Type reflect.Type

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

	// TagsToFields  maps "db" tags to struct fields.
	// Sqlair does not care about fields without a "db" tag.
	TagsToFields map[string]Field

	// FieldsToTags maps field names to tags
	FieldsToTags map[string]string
}

// GetValue returns the real, concrete value for the field name passed as
// parameter if found and true indicating it was found.  If not found, returns
// an empty interface and false.
func GetValue(info Info, arg ...string) (any, error) {
	if len(arg) > 1 {
		return nil, fmt.Errorf("Too many arguments")
	}
	var name string
	if len(arg) == 1 {
		name = arg[0]
	}
	v := info.value
	if v.Kind() == reflect.Map && v.Type().Name() == "M" {
		m := v.Interface().(M)
		k, found := m[name]
		if !found {
			return nil, fmt.Errorf("field '%s' not found", name)
		}
		return k, nil
	}
	if v.Kind() == reflect.Struct {
		f, found := info.TagsToFields[name]
		if !found {
			return nil, fmt.Errorf("field '%s' not found", name)
		}
		return reflect.Indirect(v).Field(f.Index).Interface(), nil
	}

	return v.Interface(), nil
}

// SetValue sets the field corresponding to the tag name passed as
// paremeter, to the value "value" passed as parameter. Returns true if the
// value was set, false otherwise.
func SetValue(obj any, args ...any) error {
	nargs := len(args)
	if nargs > 2 {
		return fmt.Errorf("Too many arguments")
	}
	var name string
	var value any
	if nargs <= 2 {
		value = args[0]
	}
	if nargs == 2 {
		name = args[0].(string)
		value = args[1]
	}
	m := reflect.Indirect(reflect.ValueOf(obj))
	// For sqlair.M type
	if m.Kind() == reflect.Map && m.Type().Name() == "M" {
		kv := reflect.ValueOf(name)
		vfound := m.MapIndex(kv)
		if !vfound.IsValid() {
			return fmt.Errorf("'%s' key not found in map", name)
		}
		mapValue := reflect.ValueOf(value)
		m.SetMapIndex(kv, mapValue)
		return nil
	}

	// For struct type
	i, _ := GetTypeInfo(obj)
	if i.value.Kind() == reflect.Struct {
		field, found := i.TagsToFields[name]
		if !found {
			return fmt.Errorf("field '%s' not found", name)
		}

		if field.Type != reflect.TypeOf(value) {
			return fmt.Errorf("type missmatch")
		}

		s := reflect.ValueOf(obj).Elem()

		if !s.Field(field.Index).CanSet() {
			return fmt.Errorf("%s (%s) is not settable", field.Name, name)
		}

		s.Field(field.Index).Set(reflect.ValueOf(value))
		return nil
	}

	// For simple types
	p := reflect.ValueOf(obj)
	v := p.Elem()
	v.Set(reflect.ValueOf(value))
	return nil
}

type M map[string]any
