package rpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"log"
	"reflect"

	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	mvccpb "github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/pingcap/tidb/kv"
	"github.com/powerispower/TiDB-Hackathon2018/etcdserver"
	"github.com/powerispower/TiDB-Hackathon2018/etcdserver/mvcc"
	goctx "golang.org/x/net/context"
)

const (
	Tombstone = int64(1)
)

var DBNamespace = []byte("/db")
var SysNamespace = []byte("/sys")

type kvServer struct {
	store kv.Storage
}

func NewKVServer(s *etcdserver.EtcdServer) pb.KVServer {
	return &kvServer{store: s.Store}
}

func (s *kvServer) Range(ctx context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	if err := checkRangeRequest(r); err != nil {
		return nil, err
	}

	log.Printf("Range Key=%v, RangeEnd=%v, Limit=%v, Revision=%v",
		string(r.Key), string(r.RangeEnd), r.Limit, r.Revision)

	tx, err := s.store.Begin()
	if err != nil {
		return nil, err
	}

	if r.Revision > 0 {
		// 指定版本的情况下判断给定的revision是否已经compact
		isCompacted, err := IsCompacted(r.Revision, tx)
		if err != nil {
			return nil, err
		}
		if isCompacted {
			return nil, errors.New("required revision has been compacted")
		}
	}

	var flatKey []byte = nil
	if bytes.Compare(r.Key, []byte("\000")) == 0 {
		flatKey = nil
	} else {
		key := &mvcc.Key{
			NameSpace: DBNamespace,
			RawKey:    r.Key,
			Revision:  0,
			Flag:      0,
		}
		flatKey = key.ToBytes()
	}
	var flatRangeEnd []byte = nil
	if r.RangeEnd == nil || len(r.RangeEnd) == 0 {
		rangeEnd := &mvcc.Key{
			NameSpace: DBNamespace,
			RawKey:    r.Key,
			Revision:  1<<63 - 1,
			Flag:      1<<63 - 1,
		}
		flatRangeEnd = rangeEnd.ToBytes()
	} else if bytes.Compare(r.RangeEnd, []byte("\000")) == 0 {
		flatRangeEnd = nil
	} else {
		rangeEnd := &mvcc.Key{
			NameSpace: DBNamespace,
			RawKey:    r.RangeEnd,
			Revision:  1<<63 - 1,
			Flag:      1<<63 - 1,
		}
		flatRangeEnd = rangeEnd.ToBytes()
	}

	// log.Printf("flatKey=%v, flatRangeEnd=%v\n", flatKey, flatRangeEnd)
	it, err := tx.Iter(flatKey, flatRangeEnd)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	rep := &pb.RangeResponse{
		Header: &pb.ResponseHeader{
			Revision: int64(tx.StartTS()),
		},
	}
	lastRawKey := []byte{}
	// createRevision := 0
	for it.Valid() {
		key, err := mvcc.NewKey(it.Key())
		if err != nil {
			return nil, err
		}

		if !IsNameSpace(DBNamespace, key) {
			break
		}

		itKv := &mvccpb.KeyValue{
			Key:         key.RawKey,
			Value:       it.Value(),
			ModRevision: key.Revision,
		}
		log.Printf("it Key=%v, Value=%v, Revision=%v, Flag=%v\n",
			string(key.RawKey), string(itKv.Value), key.Revision, key.Flag)

		if r.Revision > 0 && key.Revision > r.Revision {
			// if user specify revision
			// user can't see whaterver > r.Revision
		} else {
			if key.Flag == Tombstone {
				// if it is tombstone, remove all this key old revision
				i := len(rep.Kvs) - 1
				for ; i >= 0; i-- {
					if !reflect.DeepEqual(rep.Kvs[i].Key, itKv.Key) {
						break
					}
				}
				rep.Kvs = rep.Kvs[:i+1]
			} else {
				if bytes.Compare(key.RawKey, lastRawKey) == 0 {
					// replace with high revision itKv
					rep.Kvs[len(rep.Kvs)-1] = itKv
				} else {
					rep.Kvs = append(rep.Kvs, itKv)
				}
			}

			if bytes.Compare(key.RawKey, lastRawKey) != 0 {
				// createRevision =
				lastRawKey = key.RawKey
			}
		}

		it.Next()
	}

	return rep, nil
}

func (s *kvServer) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	if err := checkPutRequest(r); err != nil {
		return nil, err
	}

	tx, err := s.store.Begin()
	if err != nil {
		return nil, err
	}

	key := &mvcc.Key{
		NameSpace: DBNamespace,
		RawKey:    r.Key,
		Revision:  int64(tx.StartTS()),
		Flag:      0,
	}
	flatKey := key.ToBytes()

	err = tx.Set(flatKey, r.Value)
	if err != nil {
		return nil, err
	}

	err = tx.Commit(goctx.Background())
	if err != nil {
		return nil, err
	}
	log.Printf("Put key=%v, revision=%v,value=%v success\n", string(r.Key), key.Revision, string(r.Value))

	return &pb.PutResponse{}, nil
}

func (s *kvServer) DeleteRange(ctx context.Context, r *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	if err := checkDeleteRequest(r); err != nil {
		return nil, err
	}
	tx, err := s.store.Begin()
	if err != nil {
		return nil, err
	}
	beginKey := &mvcc.Key{
		NameSpace: DBNamespace,
		RawKey:    r.Key,
		Revision:  0,
		Flag:      0,
	}
	flatBeginKey := beginKey.ToBytes()
	endKey := &mvcc.Key{
		NameSpace: DBNamespace,
		RawKey:    r.Key,
		Revision:  1<<63 - 1,
		Flag:      1<<63 - 1,
	}
	flatEndKey := endKey.ToBytes()
	if r.RangeEnd != nil && len(flatEndKey) != 0 {
		if bytes.Compare(r.RangeEnd, []byte("\000")) == 0 {
			flatEndKey = nil
		} else {
			endKey.RawKey = r.RangeEnd
			flatEndKey = endKey.ToBytes()
		}
	}
	it, err := tx.Iter(flatBeginKey, flatEndKey)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	keyMap := make(map[string]map[string]int64)
	valueMap := make(map[string][]byte)
	for it.Valid() {
		tmpKey, err := mvcc.NewKey(it.Key())
		if err != nil {
			return nil, err
		}
		if !IsNameSpace(DBNamespace, tmpKey) {
			break
		}
		stringKey := string(tmpKey.RawKey)
		if tmpKey.Flag == 0 {
			oldValue, ok := keyMap[stringKey]
			newValue := make(map[string]int64)
			if ok {
				newValue["createRevision"] = oldValue["createRevision"]
				newValue["modRevision"] = tmpKey.Revision
			} else {
				newValue["createRevision"] = tmpKey.Revision
				newValue["modRevision"] = tmpKey.Revision
			}
			keyMap[stringKey] = newValue
			if r.PrevKv {
				valueMap[stringKey] = it.Value()
			}
		} else if tmpKey.Flag == Tombstone {
			delete(keyMap, stringKey)
			if r.PrevKv {
				delete(valueMap, stringKey)
			}
		}
		it.Next()
	}
	rep := &pb.DeleteRangeResponse{
		Header:  &pb.ResponseHeader{},
		Deleted: int64(len(keyMap)),
	}
	for stringKey, keyMapValue := range keyMap {
		log.Printf("delete key=%v", stringKey)
		deleteKey := &mvcc.Key{
			NameSpace: DBNamespace,
			RawKey:    []byte(stringKey),
			Revision:  int64(tx.StartTS()),
			Flag:      Tombstone,
		}
		err := tx.Set(deleteKey.ToBytes(), []byte("124"))
		if err != nil {
			return nil, err
		}
		// log.Printf("it Key=%v, Revision=%v, Flag=%v,",
		// 	string(deleteKey.RawKey), deleteKey.Revision, deleteKey.Flag, deleteKey.ToBytes())
		if r.PrevKv {
			keyValue := &mvccpb.KeyValue{
				Key:            []byte(stringKey),
				Value:          valueMap[stringKey],
				CreateRevision: keyMapValue["createRevision"],
				ModRevision:    keyMapValue["modRevision"],
			}
			rep.PrevKvs = append(rep.PrevKvs, keyValue)
		}
	}
	err = tx.Commit(goctx.Background())
	if err != nil {
		return nil, err
	}
	log.Printf("delete success")
	return rep, nil
}

func (s *kvServer) Txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error) {
	return &pb.TxnResponse{}, nil
}

func (s *kvServer) Compact(ctx context.Context, r *pb.CompactionRequest) (*pb.CompactionResponse, error) {
	log.Printf("enter compact")
	if err := checkCompactRequest(r); err != nil {
		return nil, err
	}
	log.Printf("checkCompactRequest ok")

	tx, err := s.store.Begin()
	if err != nil {
		return nil, err
	}
	// 判断给定的revison是否是未来的revison
	nowRevision := int64(tx.StartTS()) // TODO: use tx.CurrentVersion
	if r.Revision > nowRevision {
		return nil, errors.New("required revision is a future revision")
	}

	// 判断给定的revision是否已经compact
	isCompacted, err := IsCompacted(r.Revision, tx)
	if err != nil {
		return nil, err
	}
	if isCompacted {
		return nil, errors.New("required revision has been compacted")
	}

	// update compactPos
	compactPosKey := &mvcc.Key{
		NameSpace: SysNamespace,
		RawKey:    []byte("compactPos"),
	}
	revisionBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(revisionBytes, uint64(r.Revision))
	err = tx.Set(compactPosKey.ToBytes(), revisionBytes)
	if err != nil {
		log.Printf("error in tx.Set(compactPosKey.ToBytes(), revisionBytes): %v", err)
		return nil, err
	}

	log.Printf("judge r.Revision ok")

	// get all key
	minKey := &mvcc.Key{
		NameSpace: DBNamespace,
		RawKey:    []byte(""), // TODO confirm
		Revision:  0,
		Flag:      0,
	}

	it, err := tx.Iter(minKey.ToBytes(), nil)
	if err != nil {
		log.Printf("error in tx.Iter(minKey.ToBytes(), nil): %v", err)
		return nil, err
	}
	defer it.Close()

	preKey := &mvcc.Key{}
	nowKey := &mvcc.Key{}
	if it.Valid() {
		preKey, err = mvcc.NewKey(it.Key())
		if err != nil {
			log.Printf("error in mvcc.NewKey(it.Key()), nil): %v", err)
			return nil, err
		}
	} else {
		log.Printf("it is valid!")
	}
	for it.Valid() {
		it.Next()
		if it.Valid() {
			nowKey, err = mvcc.NewKey(it.Key())
			if err != nil {
				log.Printf("error in mvcc.NewKey(it.Key()), nil): %v", err)
				return nil, err
			}
			if !IsNameSpace(DBNamespace, nowKey) {
				break
			}
			if preKey.Revision < r.Revision &&
				bytes.Compare(preKey.RawKey, nowKey.RawKey) == 0 &&
				nowKey.Revision <= r.Revision {
				tx.Delete(preKey.ToBytes())
				// TODO delete confirm
				log.Printf("delete Key=%v, Revision=%v\n",
					string(preKey.RawKey), preKey.Revision)
			}
			preKey = nowKey
		}
	}

	err = tx.Commit(goctx.Background())
	if err != nil {
		log.Printf("error in tx.Commit(goctx.Background()): %v", err)
		tx.Rollback()
		return nil, err
	}

	return &pb.CompactionResponse{}, nil
}

func checkCompactRequest(r *pb.CompactionRequest) error {
	// TODO
	return nil
}

func checkRangeRequest(r *pb.RangeRequest) error {
	if len(r.Key) == 0 {
		return rpctypes.ErrGRPCEmptyKey
	}
	return nil
}

func checkPutRequest(r *pb.PutRequest) error {
	if len(r.Key) == 0 {
		return rpctypes.ErrGRPCEmptyKey
	}
	if r.IgnoreValue && len(r.Value) != 0 {
		return rpctypes.ErrGRPCValueProvided
	}
	if r.IgnoreLease && r.Lease != 0 {
		return rpctypes.ErrGRPCLeaseProvided
	}
	return nil
}

func (s *kvServer) getLastRevision(rawKey []byte) (lastKey *mvcc.Key, err error) {
	tx, err := s.store.Begin()
	if err != nil {
		return lastKey, err
	}

	var flatKey []byte = nil
	key := &mvcc.Key{
		NameSpace: DBNamespace,
		RawKey:    rawKey,
		Revision:  0,
		Flag:      0,
	}
	flatKey = key.ToBytes()

	var flatRangeEnd []byte = nil
	rangeEnd := &mvcc.Key{
		NameSpace: DBNamespace,
		RawKey:    rawKey,
		Revision:  1<<63 - 1,
		Flag:      1<<63 - 1,
	}
	flatRangeEnd = rangeEnd.ToBytes()

	// TODO: Use IterReverse()
	it, err := tx.Iter(flatKey, flatRangeEnd)
	if err != nil {
		return lastKey, err
	}
	defer it.Close()

	for it.Valid() {
		lastKey, err = mvcc.NewKey(it.Key())
		if err != nil {
			return lastKey, err
		}
		log.Printf("it RawKey=%v, Revision=%v, Flag=%v\n",
			string(lastKey.RawKey), lastKey.Revision, lastKey.Flag)
		it.Next()
	}
	log.Printf("result: RawKey=%v, LastRevision=%v, Flag=%v\n",
		string(lastKey.RawKey), lastKey.Revision, lastKey.Flag)
	return lastKey, err
}

func checkDeleteRequest(r *pb.DeleteRangeRequest) error {
	if len(r.Key) == 0 {
		return rpctypes.ErrGRPCEmptyKey
	}
	return nil
}

func IsNameSpace(nameSpace []byte, key *mvcc.Key) bool {
	// 判断给定的key是否属于nameSpace
	return bytes.Compare(key.NameSpace, nameSpace) == 0
}

func IsCompacted(revision int64, tx kv.Transaction) (bool, error) {
	// 判断给定的revision是否已经compact

	compactPosKey := &mvcc.Key{
		NameSpace: SysNamespace,
		RawKey:    []byte("compactPos"),
	}

	compactPosValue, err := tx.Get(compactPosKey.ToBytes())

	if err != nil && kv.IsErrNotFound(err) {
		log.Printf("Not found compactPos before revision=%v\n", revision)
		return false, nil
	} else if err == nil {
		compactPosRevision := int64(binary.LittleEndian.Uint64(compactPosValue))
		log.Printf("compactPosValue=%v, compactPosRevision:%v, revision=%v",
			compactPosValue, compactPosRevision, revision)

		return revision < compactPosRevision, nil
	} else {
		log.Printf("err in IsCompacted:tx.Get(compactPosKey.ToBytes())")
		return false, err
	}
}

// func checkTxnRequest(r *pb.TxnRequest, maxTxnOps int) error {
// 	opc := len(r.Compare)
// 	if opc < len(r.Success) {
// 		opc = len(r.Success)
// 	}
// 	if opc < len(r.Failure) {
// 		opc = len(r.Failure)
// 	}
// 	if opc > maxTxnOps {
// 		return rpctypes.ErrGRPCTooManyOps
// 	}

// 	for _, c := range r.Compare {
// 		if len(c.Key) == 0 {
// 			return rpctypes.ErrGRPCEmptyKey
// 		}
// 	}
// 	for _, u := range r.Success {
// 		if err := checkRequestOp(u, maxTxnOps-opc); err != nil {
// 			return err
// 		}
// 	}
// 	for _, u := range r.Failure {
// 		if err := checkRequestOp(u, maxTxnOps-opc); err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }

// // checkIntervals tests whether puts and deletes overlap for a list of ops. If
// // there is an overlap, returns an error. If no overlap, return put and delete
// // sets for recursive evaluation.
// func checkIntervals(reqs []*pb.RequestOp) (map[string]struct{}, adt.IntervalTree, error) {
// 	var dels adt.IntervalTree

// 	// collect deletes from this level; build first to check lower level overlapped puts
// 	for _, req := range reqs {
// 		tv, ok := req.Request.(*pb.RequestOp_RequestDeleteRange)
// 		if !ok {
// 			continue
// 		}
// 		dreq := tv.RequestDeleteRange
// 		if dreq == nil {
// 			continue
// 		}
// 		var iv adt.Interval
// 		if len(dreq.RangeEnd) != 0 {
// 			iv = adt.NewStringAffineInterval(string(dreq.Key), string(dreq.RangeEnd))
// 		} else {
// 			iv = adt.NewStringAffinePoint(string(dreq.Key))
// 		}
// 		dels.Insert(iv, struct{}{})
// 	}

// 	// collect children puts/deletes
// 	puts := make(map[string]struct{})
// 	for _, req := range reqs {
// 		tv, ok := req.Request.(*pb.RequestOp_RequestTxn)
// 		if !ok {
// 			continue
// 		}
// 		putsThen, delsThen, err := checkIntervals(tv.RequestTxn.Success)
// 		if err != nil {
// 			return nil, dels, err
// 		}
// 		putsElse, delsElse, err := checkIntervals(tv.RequestTxn.Failure)
// 		if err != nil {
// 			return nil, dels, err
// 		}
// 		for k := range putsThen {
// 			if _, ok := puts[k]; ok {
// 				return nil, dels, rpctypes.ErrGRPCDuplicateKey
// 			}
// 			if dels.Intersects(adt.NewStringAffinePoint(k)) {
// 				return nil, dels, rpctypes.ErrGRPCDuplicateKey
// 			}
// 			puts[k] = struct{}{}
// 		}
// 		for k := range putsElse {
// 			if _, ok := puts[k]; ok {
// 				// if key is from putsThen, overlap is OK since
// 				// either then/else are mutually exclusive
// 				if _, isSafe := putsThen[k]; !isSafe {
// 					return nil, dels, rpctypes.ErrGRPCDuplicateKey
// 				}
// 			}
// 			if dels.Intersects(adt.NewStringAffinePoint(k)) {
// 				return nil, dels, rpctypes.ErrGRPCDuplicateKey
// 			}
// 			puts[k] = struct{}{}
// 		}
// 		dels.Union(delsThen, adt.NewStringAffineInterval("\x00", ""))
// 		dels.Union(delsElse, adt.NewStringAffineInterval("\x00", ""))
// 	}

// 	// collect and check this level's puts
// 	for _, req := range reqs {
// 		tv, ok := req.Request.(*pb.RequestOp_RequestPut)
// 		if !ok || tv.RequestPut == nil {
// 			continue
// 		}
// 		k := string(tv.RequestPut.Key)
// 		if _, ok := puts[k]; ok {
// 			return nil, dels, rpctypes.ErrGRPCDuplicateKey
// 		}
// 		if dels.Intersects(adt.NewStringAffinePoint(k)) {
// 			return nil, dels, rpctypes.ErrGRPCDuplicateKey
// 		}
// 		puts[k] = struct{}{}
// 	}
// 	return puts, dels, nil
// }

// func checkRequestOp(u *pb.RequestOp, maxTxnOps int) error {
// 	// TODO: ensure only one of the field is set.
// 	switch uv := u.Request.(type) {
// 	case *pb.RequestOp_RequestRange:
// 		return checkRangeRequest(uv.RequestRange)
// 	case *pb.RequestOp_RequestPut:
// 		return checkPutRequest(uv.RequestPut)
// 	case *pb.RequestOp_RequestDeleteRange:
// 		return checkDeleteRequest(uv.RequestDeleteRange)
// 	case *pb.RequestOp_RequestTxn:
// 		return checkTxnRequest(uv.RequestTxn, maxTxnOps)
// 	default:
// 		// empty op / nil entry
// 		return rpctypes.ErrGRPCKeyNotFound
// 	}
// }
