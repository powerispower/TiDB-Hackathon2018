package rpc

import (
	"bytes"
	"context"
	"google.golang.org/grpc"
	"testing"

	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	mvccpb "github.com/coreos/etcd/mvcc/mvccpb"
)

func TestKv(t *testing.T) {
	// start server
	server, serverAddr, err := launchMockServer()
	if err != nil {
		t.Fatal(err)
	}
	defer server.GracefulStop()

	// open client
	conn, err := grpc.Dial(serverAddr.String(), grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := pb.NewKVClient(conn)

	// Put
	putRequests := []*pb.PutRequest{
		{Key: []byte("foo"), Value: []byte("bar")},
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
		{Key: []byte("key4"), Value: []byte("value4")},
	}
	for _, putRequest := range putRequests {
		_, err := client.Put(context.Background(), putRequest)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Range
	rangeOps := []struct {
		r        *pb.RangeRequest
		retCount int
	}{
		{r: &pb.RangeRequest{Key: []byte("key1"), RangeEnd: []byte("key3")}, retCount: 2},
		{r: &pb.RangeRequest{Key: []byte("key2"), RangeEnd: []byte("\000")}, retCount: 3},
		{r: &pb.RangeRequest{Key: []byte("\000"), RangeEnd: []byte("\000")}, retCount: 5},
		{r: &pb.RangeRequest{Key: []byte("key7"), RangeEnd: []byte("key8")}, retCount: 0},
	}
	for _, op := range rangeOps {
		resp, err := client.Range(context.Background(), op.r)
		if err != nil {
			t.Fatal(err)
		}
		if op.retCount != len(resp.Kvs) {
			t.Fatalf("op.retCount != len(resp.Kvs)")
		}
	}

	// DeleteRange
	delOps := []struct {
		r         *pb.DeleteRangeRequest
		delKvs    []*mvccpb.KeyValue
		leftCount int
	}{
		{
			r: &pb.DeleteRangeRequest{Key: []byte("key1"), RangeEnd: []byte("key3")},
			delKvs: []*mvccpb.KeyValue{
				{Key: []byte("key1")},
				{Key: []byte("key2")},
			},
			leftCount: 3,
		},
		{
			r: &pb.DeleteRangeRequest{Key: []byte("key1"), RangeEnd: []byte("\000")},
			delKvs: []*mvccpb.KeyValue{
				{Key: []byte("key3")},
				{Key: []byte("key4")},
			},
			leftCount: 1,
		},
		{
			r: &pb.DeleteRangeRequest{Key: []byte("\000"), RangeEnd: []byte("\000")},
			delKvs: []*mvccpb.KeyValue{
				{Key: []byte("foo")},
			},
			leftCount: 0,
		},
	}
	for _, op := range delOps {
		op.r.PrevKv = true
		resp, err := client.DeleteRange(context.Background(), op.r)
		if err != nil {
			t.Fatal(err)
		}

		kvEq := func(kv1, kv2 *mvccpb.KeyValue) bool {
			return bytes.Compare(kv1.Key, kv2.Key) == 0
		}

		if len(resp.PrevKvs) != len(op.delKvs) {
			t.Fatalf("len(resp.PrevKvs) != len(op.delKvs), resp.PrevKvs=%v, op.delKvs=%v", resp.PrevKvs, op.delKvs)
		}
		for i, kv := range resp.PrevKvs {
			if !kvEq(kv, op.delKvs[i]) {
				t.Fatal("!kvEq(kv, op.delKvs[i])")
			}
		}
	}
}
