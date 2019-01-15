package mvcc

import (
	"reflect"
	"testing"
)

func TestEncoder(t *testing.T) {
	remove := []byte{0}

	e, err := NewEncoder(remove)
	if err != nil {
		t.Error(err)
	}

	raw := []byte("/db")
	encode := e.Encode(raw)

	raw2raw, err := e.Decode(encode)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(raw, raw2raw) {
		t.Error("raw != raw2raw")
	}
}
