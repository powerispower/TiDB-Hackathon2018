package rpc

import (
	"encoding/binary"
	"github.com/pingcap/tidb/kv"
	"github.com/powerispower/TiDB-Hackathon2018/etcdserver/mvcc"

	goctx "golang.org/x/net/context"
)

type MetaStore struct {
	store kv.Storage
}

func (s *MetaStore) Set(key []byte, value uint64) error {
	tx, err := s.store.Begin()
	if err != nil {
		return err
	}

	flatKey := (&mvcc.MetaKey{
		RawKey: key,
	}).ToFlatKey()

	valueBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(valueBytes, value)
	err = tx.Set(flatKey, valueBytes)
	if err != nil {
		return err
	}

	err = tx.Commit(goctx.Background())
	if err != nil {
		return err
	}

	return nil
}

func (s *MetaStore) Get(key []byte) (uint64, error) {
	tx, err := s.store.Begin()
	if err != nil {
		return 0, err
	}

	flatKey := (&mvcc.MetaKey{
		RawKey: key,
	}).ToFlatKey()
	value, err := tx.Get(flatKey)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(value), nil
}
