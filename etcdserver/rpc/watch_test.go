package rpc

import (
	"bytes"
	"context"
	"google.golang.org/grpc"
	"testing"

	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	mvccpb "github.com/coreos/etcd/mvcc/mvccpb"
)

func TestWatch(t *testing.T) {
	// start server
	server, serverAddr, err := launchMockServer()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Stop()

	// open client
	conn, err := grpc.Dial(serverAddr.String(), grpc.WithInsecure())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	kvClient := pb.NewKVClient(conn)
	watchClient := pb.NewWatchClient(conn)

	// Put Data
	putRequests := []*pb.PutRequest{
		{Key: []byte("foo"), Value: []byte("bar")},
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
		{Key: []byte("key4"), Value: []byte("value4")},
	}
	for _, putRequest := range putRequests {
		_, err := kvClient.Put(context.Background(), putRequest)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Watch
	watchOps := []struct {
		r            *pb.WatchRequest
		expectEvents []*mvccpb.Event
	}{
		{
			r: &pb.WatchRequest{
				RequestUnion: &pb.WatchRequest_CreateRequest{
					CreateRequest: &pb.WatchCreateRequest{
						Key:      []byte("key1"),
						RangeEnd: []byte("key3"),
					},
				},
			},
			expectEvents: []*mvccpb.Event{
				{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("key1")}},
				{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("key2")}},
			},
		},
	}
	for _, op := range watchOps {
		stream, err := watchClient.Watch(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer stream.CloseSend()

		if err := stream.Send(op.r); err != nil {
			t.Fatal(err)
		}

		// recv created response
		var watchId int64
		if resp, err := stream.Recv(); err != nil {
			t.Fatal(err)
		} else if !resp.Created {
			t.Fatalf("!resp.Created, resp=%v", resp)
		} else {
			watchId = resp.WatchId
		}

		// recv data response
		if resp, err := stream.Recv(); err != nil {
			t.Fatal(err)
		} else {
			if len(resp.Events) != len(op.expectEvents) {
				t.Fatalf("len(resp.Events) != len(op.expectEvents), resp.Events=%v, op.expectEvents=%v", resp.Events, op.expectEvents)
			}
			for i, re := range resp.Events {
				ee := op.expectEvents[i]
				if ee.Type != re.Type || bytes.Compare(ee.Kv.Key, re.Kv.Key) != 0 {
					t.Fatalf("ee.Type != re.Type || bytes.Compare(ee.Kv.Key, re.Kv.Key) != 0 , ee=%v, re=%v", ee, re)
				}
			}
		}

		// send cancel request
		err = stream.Send(&pb.WatchRequest{
			RequestUnion: &pb.WatchRequest_CancelRequest{
				CancelRequest: &pb.WatchCancelRequest{
					WatchId: watchId,
				},
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		// recv canceled response
		if resp, err := stream.Recv(); err != nil {
			t.Fatal(err)
		} else if !resp.Canceled {
			t.Fatalf("!resp.canceled, resp=%v", resp)
		}
	}
}
