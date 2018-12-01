package mvcc

import (
	"encoding/binary"
	"fmt"
)

// Key store_key := RawKey + Version + Revision
type Key struct {
	NameSpace []byte
	RawKey    []byte
	Version   int64
	Revision  int64
}

func NewKey(b []byte) (*Key, error) {
	if len(b) < 16 {
		return nil, fmt.Errorf("len(b) = %v < 16", len(b))
	}
	key := &Key{
		RawKey:   b[:len(b)-16],
		Version:  int64(binary.LittleEndian.Uint64(b[len(b)-16 : len(b)-8])),
		Revision: int64(binary.LittleEndian.Uint64(b[len(b)-8:])),
	}
	return key, nil
}

func (k *Key) ToBytes() []byte {
	b := make([]byte, len(k.RawKey)+16)
	copy(k.RawKey, b[:len(b)-16])
	binary.LittleEndian.PutUint64(b[len(b)-16:len(b)-8], uint64(k.Version))
	binary.LittleEndian.PutUint64(b[len(b)-8:], uint64(k.Revision))
	return b
}

type Encoder struct {
	escape byte
	remove map[byte][]byte
}

func NewEncoder(escape byte, remove []byte) *Encoder {
	e := &Encoder{
		escape: escape,
		remove: make(map[byte][]byte),
	}
	pending := []byte
	pending = append(pending, escape)
	for b := remove {
		pending = append(pending, b)
		e.remove[b] = []byte("")
	}

	for i := byte(0); i < byte(-1); i++ {
		if 
		if _, ok
	}
}

func EncodeBytes(raw []byte, escape byte, remove map[byte]bool) []byte {

	encode := []byte
	for b in raw {
		if _, ok := remove[b]; ok {
			encode = append(encode, escape, )
		}
		encode = append(encode, )
	}
}
