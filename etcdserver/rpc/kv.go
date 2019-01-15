package rpc

import (
	"bytes"
	"context"
	"github.com/pingcap/tidb/kv"
	"github.com/powerispower/TiDB-Hackathon2018/etcdserver"
	"github.com/powerispower/TiDB-Hackathon2018/etcdserver/mvcc"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"

	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	mvccpb "github.com/coreos/etcd/mvcc/mvccpb"
	goctx "golang.org/x/net/context"
)

const (
	Tombstone = int64(1)
)

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

	var flatKey []byte = nil
	if r.Key == nil || bytes.Compare(r.Key, []byte("\000")) == 0 {
		// r.Key == "\000" means NamespaceBegin
		flatKey = (&mvcc.DataKey{}).NamespaceBegin()
	} else {
		// keyBegin
		flatKey = (&mvcc.DataKey{
			RawKey: r.Key,
		}).ToFlatKey()
	}
	var flatRangeEnd []byte = nil
	if r.RangeEnd == nil || bytes.Compare(r.RangeEnd, []byte("\000")) == 0 {
		// Range all keys >= r.Key
		flatRangeEnd = (&mvcc.DataKey{}).NamespaceEnd()
	} else {
		// keyEnd
		flatRangeEnd = (&mvcc.DataKey{
			RawKey:   r.RangeEnd,
			Revision: 0,
		}).ToFlatKey()
	}

	tx, err := s.store.Begin()
	if err != nil {
		return nil, err
	}

	it, err := tx.Iter(flatKey, flatRangeEnd)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	resp := &pb.RangeResponse{
		Header: &pb.ResponseHeader{
			Revision: int64(tx.StartTS()),
		},
	}
	lastRawKey := []byte{}
	var createRevision int64 = 0
	for it.Valid() {
		itKey := &mvcc.DataKey{}
		if err := itKey.ParseFromFlatKey(it.Key()); err != nil {
			return nil, err
		}

		if r.Revision > 0 && int64(itKey.Revision) > r.Revision {
			// if user specify revision
			// user can't see whaterver > r.Revision
		} else {
			if itKey.Flag == mvcc.DataFlagTombstone {
				// if flag is tombstone, remove all old revision
				i := len(resp.Kvs) - 1
				for ; i >= 0; i-- {
					if bytes.Compare(resp.Kvs[i].Key, itKey.RawKey) != 0 {
						break
					}
				}
				resp.Kvs = resp.Kvs[:i+1]

				lastRawKey = []byte{}
				createRevision = 0
			} else {
				itKv := &mvccpb.KeyValue{
					Key:         itKey.RawKey,
					Value:       it.Value(),
					ModRevision: int64(itKey.Revision),
				}

				if bytes.Compare(itKey.RawKey, lastRawKey) == 0 {
					itKv.CreateRevision = createRevision
					// replace with high revision itKv
					resp.Kvs[len(resp.Kvs)-1] = itKv
				} else {
					itKv.CreateRevision = int64(itKey.Revision)
					resp.Kvs = append(resp.Kvs, itKv)

					lastRawKey = itKv.Key
					createRevision = itKv.CreateRevision
				}
			}
		}

		it.Next()
	}

	return resp, nil
}

func (s *kvServer) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	if err := checkPutRequest(r); err != nil {
		return nil, err
	}

	tx, err := s.store.Begin()
	if err != nil {
		return nil, err
	}

	flatKey := (&mvcc.DataKey{
		RawKey:   r.Key,
		Revision: tx.StartTS(),
		Flag:     mvcc.DataFlagAdd,
	}).ToFlatKey()

	err = tx.Set(flatKey, r.Value)
	if err != nil {
		return nil, err
	}

	err = tx.Commit(goctx.Background())
	if err != nil {
		return nil, err
	}

	return &pb.PutResponse{}, nil
}

func (s *kvServer) DeleteRange(ctx context.Context, r *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	if err := checkDeleteRequest(r); err != nil {
		return nil, err
	}

	rangeReq := &pb.RangeRequest{
		Key:      r.Key,
		RangeEnd: r.RangeEnd,
	}

	rangeResp, err := s.Range(ctx, rangeReq)
	if err != nil {
		return nil, err
	}

	tx, err := s.store.Begin()
	if err != nil {
		return nil, err
	}

	resp := &pb.DeleteRangeResponse{
		Header: &pb.ResponseHeader{
			Revision: int64(tx.StartTS()),
		},
		PrevKvs: []*mvccpb.KeyValue{},
	}
	for _, kv := range rangeResp.Kvs {
		tombstone := &mvcc.DataKey{
			RawKey:   kv.Key,
			Revision: tx.StartTS(),
			Flag:     mvcc.DataFlagTombstone,
		}

		if err := tx.Set(tombstone.ToFlatKey(), []byte("tombstone")); err != nil {
			return nil, err
		}

		if r.PrevKv {
			resp.PrevKvs = append(resp.PrevKvs, kv)
		}
	}

	err = tx.Commit(goctx.Background())
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *kvServer) Txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error) {
	return &pb.TxnResponse{}, nil
}

func (s *kvServer) Compact(ctx context.Context, r *pb.CompactionRequest) (*pb.CompactionResponse, error) {
	return &pb.CompactionResponse{}, nil
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

func checkDeleteRequest(r *pb.DeleteRangeRequest) error {
	if len(r.Key) == 0 {
		return rpctypes.ErrGRPCEmptyKey
	}
	return nil
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
