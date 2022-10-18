package metainfo

import (
	"reflect"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestReflectTypeInfo(t *testing.T) {
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

	info, err := Generate(s)
	assert.Nil(t, err)

	assert.Equal(t, reflect.Struct, info.Kind())
	assert.Equal(t, "something", info.Name())

	assert.Len(t, info.Fields, 2)

	id, ok := info.Fields["id"]
	assert.True(t, ok)
	assert.Equal(t, "ID", id.Name)
	assert.False(t, id.OmitEmpty)

	name, ok := info.Fields["name"]
	assert.True(t, ok)
	assert.Equal(t, "Name", name.Name)
	assert.True(t, name.OmitEmpty)
}

func TestSimpleType(t *testing.T) {
	type myID int
	type mystr string

	var id myID = 99
	var name mystr = "Foo"

	info_id, err := Generate(id)
	assert.Nil(t, err)
	assert.NotNil(t, info_id)

	assert.NotEqual(t, info_id.value, reflect.Struct)
	v := info_id.value
	assert.Equal(t, "myID", v.Type().Name())

	info_name, err := Generate(name)
	assert.NotNil(t, info_name)
	assert.Nil(t, err)
}

func TestReflectBadTagError(t *testing.T) {
	type something struct {
		ID int64 `db:"id,bad-juju"`
	}

	s := something{ID: 99}

	_, err := Generate(s)
	assert.Error(t, errors.New(`unexpected tag value "bad-juju"`), err)
}
