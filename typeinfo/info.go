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
func GetFieldValue(obj any, fieldName string) (any, error) {
	if reflect.ValueOf(obj).Kind() == reflect.Map && reflect.TypeOf(obj).Name() == "M" {
		v := obj.(M)
		k, found := v[fieldName]
		if !found {
			return nil, fmt.Errorf("field '%s' not found", fieldName)
		}
		return k, nil
	}
	i, _ := GetTypeInfo(obj)
	v, found := i.Fields[fieldName]
	if !found {
		return nil, fmt.Errorf("field '%s' not found", fieldName)
	}
	return reflect.ValueOf(obj).Field(v.Index).Interface(), nil
}

// SetFieldValue sets the field corresponding to the tagName passed as
// paremeter, to the value "value" passed as parameter. Returns true if the
// value was set, false otherwise.
func SetFieldValue(obj any, tagName string, value any) error {
	//m, _ := GetTypeInfo(obj)
	//fmt.Printf("%+v\n", m)
	//fmt.Printf("--Enter--\n")
	//fmt.Printf("%+v\n%+v\n", reflect.ValueOf(obj).Kind(), reflect.TypeOf(obj).Name())
	//fmt.Printf("-----\n")
	//fmt.Printf("%+v\n", reflect.Indirect(reflect.ValueOf(obj)).Type())
	//fmt.Printf("-----\n")
	//fmt.Printf("%s\n", reflect.TypeOf(obj).Name())
	//fmt.Printf("-----\n")
	//n := reflect.Indirect(reflect.ValueOf(obj)).Type().Name()
	//fmt.Printf("n: %s\n", n)
	//if n == "M" {
	//if m.Kind() == reflect.Map && n == "M" {
	//	v := obj.(*M)
	//	_, found := v[tagName]
	//	if !found {
	//		return fmt.Errorf("field '%s' not found", tagName)
	//	}
	//	v[tagName] = reflect.ValueOf(value).Interface()
	//	return nil
	//}

	i, _ := GetTypeInfo(obj)
	field, found := i.Fields[tagName]
	if !found {
		return fmt.Errorf("field '%s' not found", tagName)
	}

	if field.Type != reflect.TypeOf(value) {
		//fmt.Printf("Types: %v\n%v", field.Type, reflect.TypeOf(value))
		return fmt.Errorf("type missmatch")
	}

	s := reflect.ValueOf(obj).Elem()

	if !s.Field(field.Index).CanSet() {
		return fmt.Errorf("%s (%s) is not settable", field.Name, tagName)
	}

	s.Field(field.Index).Set(reflect.ValueOf(value))
	return nil
}

type M map[string]any
