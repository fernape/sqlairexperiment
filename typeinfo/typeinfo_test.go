package typeinfo

import (
	"reflect"
	"sync"
	"testing"

	"github.com/pkg/errors"
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

	assert.Equal(t, reflect.Int64, info.Kind())
	assert.Equal(t, "int64", info.Name())

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

func TestReflectM(t *testing.T) {
	var mymap M
	mymap = make(M)
	mymap["foo"] = 7
	mymap["bar"] = "baz"

	info, err := GetTypeInfo(mymap)
	assert.Nil(t, err)

	assert.Len(t, info.Fields, 2)
	foo, ok := info.Fields["foo"]
	assert.True(t, ok)
	assert.Equal(t, "foo", foo.Name)
}

func TestReflectBadTagError(t *testing.T) {
	type something struct {
		ID int64 `db:"id,bad-juju"`
	}

	s := something{ID: 99}

	_, err := GetTypeInfo(s)
	assert.Error(t, errors.New(`unexpected tag value "bad-juju"`), err)
}
