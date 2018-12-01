package mvcc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
)

var remove = []byte{0}

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
	if len(storeBytes) <= 16*2 {
		return nil, fmt.Errorf("len(b) = %v <= 16*2", len(storeBytes))
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
				tmp, err := globalEncoder.Decode(storeBytes[lastRemovePosition+1 : i])
				if err != nil {
					return nil, err
				}
				key.NameSpace = tmp
			case 2: //rawkey
				tmp, err := globalEncoder.Decode(storeBytes[lastRemovePosition+1 : i])
				if nil != err {
					return nil, err
				}

				key.RawKey = tmp
			case 3: //Revision
				if len(storeBytes[lastRemovePosition+1:i]) != 16 {
					return nil, fmt.Errorf("revision length != 16")
				}
				tmp, err := globalEncoder.Decode(storeBytes[lastRemovePosition+1 : i])
				if nil != err {
					return nil, err
				}

				key.Revision = int64(binary.BigEndian.Uint64(tmp))
			}
			lastRemovePosition = i
		}
	}
	if len(storeBytes[lastRemovePosition+1:]) != 16 {
		return nil, fmt.Errorf("flag length != 16")
	}
	// flag
	tmp, err := globalEncoder.Decode(storeBytes[lastRemovePosition+1:])
	if nil != err {
		return nil, err
	}

	key.Flag = int64(binary.BigEndian.Uint64(tmp))
	return key, nil
}

func (k *Key) ToBytes() []byte {
	tmp := [][]byte{
		k.NameSpace,
		k.RawKey,
	}

	// + Revision
	revision := make([]byte, 8)
	binary.BigEndian.PutUint64(revision, uint64(k.Revision))
	tmp = append(tmp, revision)

	// + Flag
	flag := make([]byte, 8)
	binary.BigEndian.PutUint64(flag, uint64(k.Flag))
	tmp = append(tmp, flag)

	for i, flatBytes := range tmp {
		tmp[i] = globalEncoder.Encode(flatBytes)
	}

	return bytes.Join(tmp, remove)
}
