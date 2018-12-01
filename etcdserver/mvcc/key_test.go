package mvcc

import (
	"reflect"
	"testing"
)

func TestKey(t *testing.T) {
	key := &Key{
		RawKey:   []byte("hello"),
		Revision: 12,
	}
	b := key.ToBytes()
	key2key, err := NewKey(b)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(key, key2key) {
		t.Error("key != key2key")
	}
}
