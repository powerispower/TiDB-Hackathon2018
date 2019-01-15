package mvcc

import (
	"reflect"
	"testing"
)

func TestDataKey(t *testing.T) {
	k := &DataKey{
		RawKey:   []byte("foo"),
		Flag:     DataFlagAdd,
		Revision: 123,
	}

	flatKey := k.ToFlatKey()
	k2k := &DataKey{}
	if err := k2k.ParseFromFlatKey(flatKey); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(k, k2k) {
		t.Fatal("!reflect.DeepEqual(k, k2k)")
	}
}

func TestTmpKey(t *testing.T) {
	k := &TmpKey{
		Directory: []byte("my_room"),
		KeepTime:  123,
		RawKey: (&DataKey{
			RawKey:   []byte("foo"),
			Flag:     DataFlagAdd,
			Revision: 1,
		}).ToFlatKey(),
	}

	flatKey := k.ToFlatKey()
	k2k := &TmpKey{}
	if err := k2k.ParseFromFlatKey(flatKey); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(k, k2k) {
		t.Fatal("!reflect.DeepEqual(k, k2k)")
	}
}

func TestMetaKey(t *testing.T) {
	k := &MetaKey{
		RawKey: []byte("hehe"),
	}

	flatKey := k.ToFlatKey()
	k2k := &MetaKey{}
	if err := k2k.ParseFromFlatKey(flatKey); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(k, k2k) {
		t.Fatal("!reflect.DeepEqual(k, k2k)")
	}
}
