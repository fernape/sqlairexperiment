package typeinfo

import (
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReflectSimpleConcurrent(t *testing.T) {
	var num int64

	wg := sync.WaitGroup{}

	// Set up some concurrent access.
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			_, _ = GetTypeInfo(num)
			wg.Done()
		}()
	}

	info, err := GetTypeInfo(num)
	assert.Nil(t, err)

	assert.Equal(t, reflect.Int64, info.value.Kind())
	assert.Equal(t, "int64", info.value.Type().Name())

	wg.Wait()
}

func TestReflectStruct(t *testing.T) {
	type something struct {
		ID      int64  `db:"id"`
		Name    string `db:"name,omitempty"`
		NotInDB string
	}

	s := something{
		ID:      99,
		Name:    "Chainheart Machine",
		NotInDB: "doesn't matter",
	}

	info, err := GetTypeInfo(s)
	assert.Nil(t, err)

	assert.Equal(t, reflect.Struct, info.value.Kind())
	assert.Equal(t, "something", info.value.Type().Name())

	assert.Len(t, info.TagsToFields, 2)

	id, ok := info.TagsToFields["id"]
	assert.True(t, ok)
	assert.Equal(t, "ID", id.Name)
	assert.False(t, id.OmitEmpty)

	name, ok := info.TagsToFields["name"]
	assert.True(t, ok)
	assert.Equal(t, "Name", name.Name)
	assert.True(t, name.OmitEmpty)
}

func TestGetSetStruct(t *testing.T) {
	type something struct {
		ID      int64  `db:"id"`
		Name    string `db:"name"`
		NotInDB string
	}

	s := something{
		ID:      99,
		Name:    "foo",
		NotInDB: "bar",
	}

	info, err := GetTypeInfo(s)
	assert.Nil(t, err)
	{
		v, err := GetValue(info, "id")
		assert.Equal(t, nil, err)
		assert.Equal(t, (int64)(99), v)
	}
	{
		v, err := GetValue(info, "nope")
		assert.Equal(t, fmt.Errorf("field 'nope' not found"), err)
		assert.Equal(t, nil, v)
	}
	{
		err := SetValue(&s, "id", (int64)(33))
		assert.Nil(t, err)
		var v any
		i, _ := GetTypeInfo(s)
		v, err = GetValue(i, "id")
		assert.Nil(t, err)
		assert.Equal(t, (int64)(33), v)
	}
	{
		err := SetValue(&s, "id", "this is a string")
		assert.Equal(t, fmt.Errorf("type missmatch"), err)
		var v any
		i, _ := GetTypeInfo(s)
		v, err = GetValue(i, "id")
		assert.Nil(t, err)
		assert.Equal(t, (int64)(33), v)
	}
}

func TestGetSetMap(t *testing.T) {
	var m = make(M)
	m["id"] = 99
	m["name"] = "Jon Doe"

	info, err := GetTypeInfo(m)
	assert.Nil(t, err)
	{
		v, err := GetValue(info, "id")
		assert.Equal(t, nil, err)
		assert.Equal(t, 99, v)
	}
	{
		v, err := GetValue(info, "nope")
		assert.Equal(t, fmt.Errorf("field 'nope' not found"), err)
		assert.Equal(t, nil, v)
	}
	//{
	//	err := SetValue(&m, "id", 33)
	//	assert.Nil(t, err)
	//	var v any
	//	v, err = GetValue(m, "id")
	//	assert.Nil(t, err)
	//	assert.Equal(t, 33, v)
	//}
	//{
	//	err := SetValue(&m, "nope", 33)
	//	assert.Equal(t, fmt.Errorf("'nope' key not found in map"), err)
	//}
}

func TestReflectM(t *testing.T) {
	var mymap M
	mymap = make(M)
	mymap["foo"] = 7
	mymap["bar"] = "baz"

	info, err := GetTypeInfo(mymap)
	assert.Nil(t, err)

	assert.Len(t, info.TagsToFields, 2)
	foo, ok := info.TagsToFields["foo"]
	assert.True(t, ok)
	assert.Equal(t, "foo", foo.Name)
}

func TestReflectBadTagError(t *testing.T) {
	type something struct {
		ID int64 `db:"id,bad-juju"`
	}

	s := something{ID: 99}

	_, err := GetTypeInfo(s)
	assert.Error(t, fmt.Errorf(`unexpected tag value "bad-juju"`), err)
}

func TestReflectSimpleTypes(t *testing.T) {
	var i int
	var s string
	var mymap map[string]string

	{
		info, err := GetTypeInfo(i)
		assert.NotEqual(t, info, Info{})
		assert.Nil(t, err)
	}
	{
		info, err := GetTypeInfo(s)
		assert.NotEqual(t, info, Info{})
		assert.Nil(t, err)
	}
	{
		info, err := GetTypeInfo(mymap)
		assert.Equal(t, info, Info{})
		assert.Equal(t, err, fmt.Errorf("Can't reflect map type"))
	}
}

func TestGetSetSimpleTypes(t *testing.T) {
	var err error
	var info Info
	{
		var vi any

		i := 99
		info, err = GetTypeInfo(i)
		assert.Nil(t, err)
		vi, err = GetValue(info)
		assert.Nil(t, err)
		assert.Equal(t, i, vi)
		err = SetValue(&i, "", 100)
		assert.Nil(t, err)
		assert.Equal(t, 100, i)
	}

	{
		var vs any

		s := "foo"
		info, err = GetTypeInfo(s)
		assert.Nil(t, err)
		vs, err = GetValue(info)
		assert.Nil(t, err)
		assert.Equal(t, s, vs)
		err = SetValue(&s, "bar")
		assert.Nil(t, err)
		assert.Equal(t, "bar", s)
	}
}
