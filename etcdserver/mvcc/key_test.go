package mvcc

import (
	"log"
	"reflect"
	"testing"
)

func TestKey(t *testing.T) {
	mockKey := &Key{}
	mockKey.NameSpace = []byte("/db")
	mockKey.RawKey = []byte("foo")
	mockKey.Revision = 1
	mockKey.Flag = 2

	mockBytes := mockKey.ToBytes()
	key, err := NewKey(mockBytes)
	if nil != err {
		log.Printf(err.Error())
		t.Error("call NewKey failed")
	}
	if !reflect.DeepEqual(key.NameSpace, mockKey.NameSpace) {
		t.Error("namespace check failed")
	}
	if !reflect.DeepEqual(key.RawKey, mockKey.RawKey) {
		t.Error("RawKey check failed")
	}
	if key.Revision != mockKey.Revision {
		t.Error("revision check failed")
	}
	if key.Flag != mockKey.Flag {
		t.Error("flag check failed")
	}
}
