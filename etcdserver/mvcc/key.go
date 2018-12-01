package mvcc

import (
	"encoding/binary"
	"fmt"
	"log"
)

const (
	remove = "a"
)

var globalEncoder *Encoder
var removeBytes []byte

func init() {
	removeBytes = []byte(remove)
	encoder, err := NewEncoder(removeBytes)
	if nil != err {
		log.Printf("new encoder failed")
	}
	globalEncoder = encoder
}

// Key tikv_key = NameSpace$RawKey$Revision$Flag
type Key struct {
	NameSpace []byte
	RawKey    []byte
	Revision  int64
	Flag      int64
}

func NewKey(storeBytes []byte) (*Key, error) {
	if len(storeBytes) < 16 {
		return nil, fmt.Errorf("len(b) = %v < 16", len(storeBytes))
	}
	key := &Key{}
	removeCnt := 0
	lastRemovePosition := -1
	for i, value := range storeBytes {
		if value == removeBytes[0] {
			removeCnt += 1
			if 0 == len(storeBytes[lastRemovePosition+1:i]) {
				return nil, fmt.Errorf("exist Key attributes bytes length = 0")
			}
			switch removeCnt {
			case 1: // namepace
				key.NameSpace = storeBytes[lastRemovePosition+1 : i]
			case 2: //rawkey
				realKey, err := globalEncoder.Decode(storeBytes[lastRemovePosition+1 : i])
				if nil != err {
					return nil, err
				}
				key.RawKey = realKey
			case 3: //Revision
				if 8 != len(storeBytes[lastRemovePosition+1:i]) {
					return nil, fmt.Errorf("revision length != 8")
				}
				key.Revision = int64(binary.BigEndian.Uint64(storeBytes[lastRemovePosition+1 : i]))
			}
			lastRemovePosition = i
		}
	}
	if 8 != len(storeBytes[lastRemovePosition+1:]) {
		return nil, fmt.Errorf("flag length != 8")
	}
	// flag
	key.Flag = int64(binary.BigEndian.Uint64(storeBytes[lastRemovePosition+1:]))
	return key, nil
}

func (key *Key) ToBytes() []byte {
	ret := make([]byte, len(key.NameSpace))
	copy(ret, key.NameSpace)
	encodeRawKey := globalEncoder.Encode(key.RawKey)
	ret = append(ret, removeBytes[0])
	ret = append(ret, encodeRawKey...)
	revisionBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(revisionBytes, uint64(key.Revision))
	ret = append(ret, removeBytes[0])
	ret = append(ret, revisionBytes...)
	flagBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(flagBytes, uint64(key.Flag))
	ret = append(ret, removeBytes[0])
	ret = append(ret, flagBytes...)
	return ret
}
