package rpc

import (
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore"
	"testing"
)

func TestMetaStore(t *testing.T) {
	// open store
	var driver mockstore.MockDriver
	store, err := driver.Open("mocktikv://")
	if err != nil {
		t.Fatal(err)
	}

	s := &MetaStore{
		store: store,
	}

	key := []byte("CompactRevision")
	value := uint64(233)

	// test get non-exist key
	if _, err := s.Get(key); !kv.IsErrNotFound(err) {
		t.Fatalf("!kv.IsErrNotFound, err=%v, key=%v", err, key)
	}

	// set key, value
	if err := s.Set(key, value); err != nil {
		t.Fatal(err)
	}

	// check key, value
	getValue, err := s.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if getValue != value {
		t.Fatalf("getValue != value, getValue=%v", getValue)
	}
}
