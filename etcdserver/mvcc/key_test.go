package mvcc

import (
	"log"
	"reflect"
	"testing"
)

func TestGroupedParallel(t *testing.T) {
	key := &Key{
		RawKey:   []byte("hello"),
		Version:  11,
		Revision: 12,
	}
	b := key.ToBytes()
	key2key, err := NewKey(b)
	log.Println(key, b, key2key)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(key, key2key) {
		t.Error("key != key2key")
	}
}
