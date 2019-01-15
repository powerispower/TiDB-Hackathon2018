package mvcc

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const Separator byte = 0
const MaxRevision uint64 = 1<<63 - 1
const MaxKeepTime uint64 = 1<<63 - 1

var keyEncoder *Encoder

func init() {
	var err error
	keyEncoder, err = NewEncoder([]byte{Separator})
	if nil != err {
		panic(err)
	}
}

type Key interface {
	Namespace() []byte
	ParseFromFlatKey(flatKey []byte) error
	ToFlatKey() []byte
}

type DataFlagType uint64

const (
	DataFlagAdd       DataFlagType = 0
	DataFlagTombstone DataFlagType = 1
)

// DataKey :
// Namespace $ RawKey $ Revision $ Flag
type DataKey struct {
	RawKey   []byte
	Revision uint64
	Flag     DataFlagType
}

func (k *DataKey) Namespace() []byte {
	return []byte("/data")
}

func (k *DataKey) ParseFromFlatKey(flatKey []byte) error {
	elements := bytes.Split(flatKey, []byte{Separator})
	if len(elements) != 4 {
		return fmt.Errorf("len(elements) != 4, len(elements)=%d", len(elements))
	}

	namespace, err := keyEncoder.Decode(elements[0])
	if err != nil {
		return err
	}
	if bytes.Compare(namespace, k.Namespace()) != 0 {
		return fmt.Errorf("bytes.Compare(namespace, k.Namespace()) != 0, namespace=%v", namespace)
	}

	k.RawKey, err = keyEncoder.Decode(elements[1])
	if err != nil {
		return err
	}

	revision, err := keyEncoder.Decode(elements[2])
	if err != nil {
		return err
	}
	k.Revision = binary.BigEndian.Uint64(revision)

	flag, err := keyEncoder.Decode(elements[3])
	if err != nil {
		return err
	}
	k.Flag = DataFlagType(binary.BigEndian.Uint64(flag))

	return nil
}

// ToFlatKey FlatKey = Separator.join({Namespace, RawKey, Revision, Flag})
func (k *DataKey) ToFlatKey() []byte {
	segs := [][]byte{
		k.Namespace(),
		k.RawKey,
	}

	// + Revision
	revision := make([]byte, 8)
	binary.BigEndian.PutUint64(revision, uint64(k.Revision))
	segs = append(segs, revision)

	// + Flag
	flag := make([]byte, 8)
	binary.BigEndian.PutUint64(flag, uint64(k.Flag))
	segs = append(segs, flag)

	for i, seg := range segs {
		segs[i] = keyEncoder.Encode(seg)
	}

	return bytes.Join(segs, []byte{Separator})
}

func (k *DataKey) NamespaceBegin() []byte {
	return keyEncoder.Encode(k.Namespace())
}

func (k *DataKey) NamespaceEnd() []byte {
	return append(k.NamespaceBegin(), Separator+1)
}

// TmpKey :
// Namespace $ Directory $ KeepTime $ RawKey
type TmpKey struct {
	Directory []byte
	KeepTime  uint64
	RawKey    []byte
}

func (k *TmpKey) Namespace() []byte {
	return []byte("/tmp")
}

func (k *TmpKey) ParseFromFlatKey(flatKey []byte) error {
	elements := bytes.Split(flatKey, []byte{Separator})
	if len(elements) != 4 {
		return fmt.Errorf("len(elements) != 4, len(elements)=%d", len(elements))
	}

	// => k.Namespace
	namespace, err := keyEncoder.Decode(elements[0])
	if err != nil {
		return err
	}
	if bytes.Compare(namespace, k.Namespace()) != 0 {
		return fmt.Errorf("bytes.Compare(namespace, k.Namespace()) != 0, namespace=%v", namespace)
	}

	// => k.Directory
	k.Directory, err = keyEncoder.Decode(elements[1])
	if err != nil {
		return err
	}

	// => k.KeepTime
	keepTimeBytes, err := keyEncoder.Decode(elements[2])
	if err != nil {
		return err
	}
	k.KeepTime = binary.BigEndian.Uint64(keepTimeBytes)

	k.RawKey, err = keyEncoder.Decode(elements[3])
	if err != nil {
		return err
	}

	return nil
}

func (k *TmpKey) ToFlatKey() []byte {
	segs := [][]byte{
		k.Namespace(),
		k.Directory,
	}

	// + KeepTime
	keepTime := make([]byte, 8)
	binary.BigEndian.PutUint64(keepTime, uint64(k.KeepTime))
	segs = append(segs, keepTime)

	// + Revision
	segs = append(segs, k.RawKey)

	for i, seg := range segs {
		segs[i] = keyEncoder.Encode(seg)
	}

	return bytes.Join(segs, []byte{Separator})
}

type MetaKey struct {
	RawKey []byte
}

func (k *MetaKey) Namespace() []byte {
	return []byte("/meta")
}

func (k *MetaKey) ParseFromFlatKey(flatKey []byte) error {
	elements := bytes.Split(flatKey, []byte{Separator})
	if len(elements) != 2 {
		return fmt.Errorf("len(elements) != 4, len(elements)=%d", len(elements))
	}

	// => k.Namespace
	namespace, err := keyEncoder.Decode(elements[0])
	if err != nil {
		return err
	}
	if bytes.Compare(namespace, k.Namespace()) != 0 {
		return fmt.Errorf("bytes.Compare(namespace, k.Namespace()) != 0, namespace=%v", namespace)
	}

	// => k.RawKey
	k.RawKey, err = keyEncoder.Decode(elements[1])
	if err != nil {
		return err
	}

	return nil
}

func (k *MetaKey) ToFlatKey() []byte {
	segs := [][]byte{
		k.Namespace(),
		k.RawKey,
	}

	for i, seg := range segs {
		segs[i] = keyEncoder.Encode(seg)
	}

	return bytes.Join(segs, []byte{Separator})
}
