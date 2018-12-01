package mvcc

import (
	"log"
	"reflect"
	"testing"
)

func TestEncoder(t *testing.T) {
	remove := []byte{1, 3}

	e, err := NewEncoder(remove)
	if err != nil {
		t.Error(err)
	}

	raw := []byte{0, 1, 1, 2, 3, 5, 8}
	encode := e.Encode(raw)
	log.Printf("raw=%v, encode=%v", raw, encode)

	raw2raw, err := e.Decode(encode)
	if err != nil {
		t.Error(err)
	}
	log.Printf("raw2raw=%v", raw2raw)

	if !reflect.DeepEqual(raw, raw2raw) {
		t.Error("raw != raw2raw")
	}
}
