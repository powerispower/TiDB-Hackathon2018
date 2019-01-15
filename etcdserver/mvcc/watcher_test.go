package mvcc

import (
	"bytes"
	"reflect"
	"testing"
)

func TestOrderedByRevisionEncode(t *testing.T) {
	k1 := &DataKey{
		RawKey:   []byte("abc"),
		Flag:     DataFlagAdd,
		Revision: 2,
	}
	k2 := &DataKey{
		RawKey:   []byte("bbc"),
		Flag:     DataFlagAdd,
		Revision: 1,
	}

	encodeK1 := orderedByRevisionEncode(k1)
	encodeK2 := orderedByRevisionEncode(k2)

	if bytes.Compare(encodeK1, encodeK2) <= 0 {
		t.Fatal("order failed")
	}

	ck1, err := orderedByRevisionDecode(encodeK1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(k1, ck1) {
		t.Fatalf("!reflect.DeepEqual(k, k2k), k1=%v, ck1=%v", k1, ck1)
	}
}
