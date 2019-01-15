package mvcc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/pingcap/tidb/kv"
	"log"
	"time"

	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	mvccpb "github.com/coreos/etcd/mvcc/mvccpb"
	goctx "golang.org/x/net/context"
)

type Watcher struct {
	Store         kv.Storage
	Key           []byte
	RangeEnd      []byte
	StartRevision uint64
	CtrlStream    chan *pb.WatchRequest
	EventStream   chan *pb.WatchResponse
	WatchId       int64
}

func (w *Watcher) sweepEvent() error {
	var flatKey []byte = nil
	if w.Key == nil || bytes.Compare(w.Key, []byte("\000")) == 0 {
		// w.Key == "\000" means NamespaceBegin
		flatKey = (&DataKey{}).NamespaceBegin()
	} else {
		// keyBegin
		flatKey = (&DataKey{
			RawKey: w.Key,
		}).ToFlatKey()
	}
	var flatRangeEnd []byte = nil
	if w.RangeEnd == nil || bytes.Compare(w.RangeEnd, []byte("\000")) == 0 {
		// Range all keys >= w.Key
		flatRangeEnd = (&DataKey{}).NamespaceEnd()
	} else {
		// keyEnd
		flatRangeEnd = (&DataKey{
			RawKey: w.RangeEnd,
		}).ToFlatKey()
	}

	const SweepKeepTime = 0

	sortRoom := []byte(fmt.Sprintf("watch/%d/sort", w.WatchId))

	// range watch key & sort key by revision
	rangeWatchKeyAndSetInSortRoom := func() error {
		tx, err := w.Store.Begin()
		if err != nil {
			return err
		}
		it, err := tx.Iter(flatKey, flatRangeEnd)
		if err != nil {
			return err
		}
		defer it.Close()

		for it.Valid() {
			itKey := &DataKey{}
			if err := itKey.ParseFromFlatKey(it.Key()); err != nil {
				return err
			}

			if w.StartRevision <= itKey.Revision {
				writeTx, err := w.Store.Begin()
				if err != nil {
					log.Printf("w.Store.Begin failed, err=%v", err)
					return err
				}
				tmpKey := &TmpKey{
					Directory: sortRoom,
					KeepTime:  SweepKeepTime,
					RawKey:    orderedByRevisionEncode(itKey),
				}
				if err := writeTx.Set(tmpKey.ToFlatKey(), it.Value()); err != nil {
					return err
				}

				if err := writeTx.Commit(goctx.Background()); err != nil {
					log.Printf("writeTx.Commit failed, err=%v", err)
					return err
				}
			}

			it.Next()
		}

		return nil
	}
	if err := rangeWatchKeyAndSetInSortRoom(); err != nil {
		log.Printf("rangeWatchKeyAndSetInSortRoom failed, err=%v", err)
		return err
	}

	tx, err := w.Store.Begin()
	if err != nil {
		log.Printf("w.Store.Begin failed, err=%v", err)
		return err
	}
	sortRoomBegin := (&TmpKey{
		Directory: sortRoom,
		KeepTime:  SweepKeepTime,
	}).ToFlatKey()
	sortRoomEnd := (&TmpKey{
		Directory: sortRoom,
		KeepTime:  MaxKeepTime,
	}).ToFlatKey()
	it, err := tx.Iter(sortRoomBegin, sortRoomEnd)
	if err != nil {
		log.Printf("tx.Iter failed, err=%v", err)
		return err
	}
	defer it.Close()

	resp := &pb.WatchResponse{
		Events: []*mvccpb.Event{},
	}
	// range sorted watch key
	for it.Valid() {
		// delete key
		if err := tx.Delete(it.Key()); err != nil {
			return err
		}

		itKey := &TmpKey{}
		if err := itKey.ParseFromFlatKey(it.Key()); err != nil {
			log.Printf("itKey.ParseFromFlatKey failed, err=%v", err)
			return err
		}

		dataKey, err := orderedByRevisionDecode(itKey.RawKey)
		if err != nil {
			log.Printf("orderedByRevisionDecode failed, err=%v", err)
			return err
		}

		var event *mvccpb.Event

		switch dataKey.Flag {
		case DataFlagAdd:
			event = &mvccpb.Event{
				Type: mvccpb.PUT,
				Kv: &mvccpb.KeyValue{
					Key:         dataKey.RawKey,
					Value:       it.Value(),
					ModRevision: int64(dataKey.Revision),
				},
			}
		case DataFlagTombstone:
			event = &mvccpb.Event{
				Type: mvccpb.DELETE,
				Kv: &mvccpb.KeyValue{
					Key:         dataKey.RawKey,
					ModRevision: int64(dataKey.Revision),
				},
			}
		default:
			return fmt.Errorf("invalid dataKey.Flag, dataKey.Flag=%v", dataKey.Flag)
		}

		resp.Events = append(resp.Events, event)

		it.Next()
	}
	if len(resp.Events) > 0 {
		w.EventStream <- resp

		for _, e := range resp.Events {
			if uint64(e.Kv.ModRevision) >= w.StartRevision {
				w.StartRevision = uint64(e.Kv.ModRevision) + 1
			}
		}
	}

	if err := tx.Commit(goctx.Background()); err != nil {
		log.Printf("tx.Commit failed, err=%v", err)
		return err
	}

	return nil
}

// Watch (w.Key, w.RangeEnd] and send events to w.EventStream
func (w *Watcher) Watch() error {
	defer close(w.EventStream)

	w.EventStream <- &pb.WatchResponse{
		WatchId: w.WatchId,
		Created: true,
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		// recv command from w.CtrlStream
		case req, ok := <-w.CtrlStream:
			if !ok {
				return nil
			}

			switch req.RequestUnion.(type) {
			case *pb.WatchRequest_CreateRequest:
			case *pb.WatchRequest_CancelRequest:
				w.EventStream <- &pb.WatchResponse{
					WatchId:      w.WatchId,
					Canceled:     true,
					CancelReason: fmt.Sprintf("user command stop"),
				}
				return nil
			}
		// sweep event every ticker.C
		case <-ticker.C:
			if err := w.sweepEvent(); err != nil {
				w.EventStream <- &pb.WatchResponse{
					WatchId:      w.WatchId,
					Canceled:     true,
					CancelReason: fmt.Sprintf("w.Watch failed, err=%v", err),
				}
				return err
			}
		}
	}

	return nil
}

func orderedByRevisionEncode(k *DataKey) []byte {
	revision := make([]byte, 8)
	binary.BigEndian.PutUint64(revision, uint64(k.Revision))

	flag := make([]byte, 8)
	binary.BigEndian.PutUint64(flag, uint64(k.Flag))

	return bytes.Join([][]byte{
		revision, k.RawKey, flag,
	}, []byte{})
}

func orderedByRevisionDecode(b []byte) (*DataKey, error) {
	if len(b) < 16 {
		return nil, fmt.Errorf("len(b) < 16, len(b)=%d", len(b))
	}

	k := &DataKey{
		Revision: binary.BigEndian.Uint64(b[:8]),
		RawKey:   b[8 : len(b)-8],
		Flag:     DataFlagType(binary.BigEndian.Uint64(b[len(b)-8:])),
	}

	return k, nil
}
