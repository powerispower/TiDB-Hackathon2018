package mvcc

import (
	"fmt"
	"log"
)

type Encoder struct {
	encodeBook [][2]byte
	decodeBook map[[2]byte]byte
}

func NewEncoder(remove []byte) (*Encoder, error) {
	e := &Encoder{
		encodeBook: [][2]byte{},
		decodeBook: make(map[[2]byte]byte),
	}
	removeBook := make(map[byte]bool)
	for _, b := range remove {
		removeBook[b] = true
	}

	for i := 0; i < int(^uint16(0)); i++ {
		low8bit := byte(i & 0xFF)
		if _, ok := removeBook[low8bit]; ok {
			continue
		}
		high8bit := byte((i & 0xFFFF) >> 8)
		if _, ok := removeBook[high8bit]; ok {
			continue
		}
		rawByte := byte(len(e.encodeBook))
		encodeByte := [2]byte{high8bit, low8bit}
		e.encodeBook = append(e.encodeBook, encodeByte)
		e.decodeBook[encodeByte] = rawByte
		if len(e.encodeBook) > int(^byte(0)) {
			break
		}
	}
	if len(e.encodeBook) != int(^byte(0))+1 {
		return nil, fmt.Errorf("len(e.encodeBook)=%v", len(e.encodeBook))
	}

	return e, nil
}

func (e *Encoder) Encode(raw []byte) []byte {
	encode := []byte{}
	for _, b := range raw {
		encode = append(encode, e.encodeBook[b][:]...)
	}
	return encode
}

func (e *Encoder) Decode(encode []byte) ([]byte, error) {
	if len(encode)&1 != 0 {
		return nil, fmt.Errorf("encode bytes must be multiple of 2")
	}

	raw := []byte{}
	for i := 0; i < len(encode); i += 2 {
		code := [2]byte{encode[i], encode[i+1]}
		if b, ok := e.decodeBook[code]; ok {
			raw = append(raw, b)
		} else {
			return nil, fmt.Errorf("invalid code=%v, e.decodeBook=%v", code, e.decodeBook)
		}
	}
	return raw, nil
}
