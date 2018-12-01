package mvcc

import (
	"encoding/binary"
	"fmt"
)

// Key tikv_key = NameSpace$RawKey$Revision$Flag
type Key struct {
	NameSpace []byte
	RawKey    []byte
	Revision  int64
	Flag      int64
}

// NewKey tikv_key => etcd Key
func NewKey(b []byte) (*Key, error) {
	if len(b) < 16 {
		return nil, fmt.Errorf("len(b) = %v < 16", len(b))
	}
	key := &Key{
		RawKey:   b[:len(b)-16],
		Revision: int64(binary.LittleEndian.Uint64(b[len(b)-8:])),
	}
	return key, nil
}

func (k *Key) ToBytes() []byte {
	b := make([]byte, len(k.RawKey)+16)
	copy(k.RawKey, b[:len(b)-16])
	binary.LittleEndian.PutUint64(b[len(b)-8:], uint64(k.Revision))
	return b
}
