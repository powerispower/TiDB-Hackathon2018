package rpc

import (
	"fmt"
	"github.com/pingcap/tidb/kv"
	"github.com/powerispower/TiDB-Hackathon2018/etcdserver"
	"github.com/powerispower/TiDB-Hackathon2018/etcdserver/mvcc"
	"log"
	"sync"
	"time"

	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

type watchServer struct {
	store kv.Storage
}

func NewWatchServer(s *etcdserver.EtcdServer) pb.WatchServer {
	return &watchServer{store: s.Store}
}

func (ws *watchServer) Watch(stream pb.Watch_WatchServer) (err error) {
	sws := &serverWatchStream{
		grpcStream:  stream,
		eventStream: make(chan *pb.WatchResponse),
		store:       ws.store,
		watcherBook: map[int64]*mvcc.Watcher{},
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		if tmpErr := sws.SendLoop(); tmpErr != nil {
			err = tmpErr
		}
	}()

	// Accept the command and execute
	if tmpErr := sws.RecvLoop(); tmpErr != nil {
		err = tmpErr
	}

	// wait SendLoop done
	wg.Wait()

	return err
}

type serverWatchStream struct {
	sync.RWMutex
	sync.WaitGroup

	grpcStream  pb.Watch_WatchServer
	eventStream chan *pb.WatchResponse
	// eventStream reference count
	esRefCount sync.WaitGroup

	store       kv.Storage
	watcherBook map[int64]*mvcc.Watcher
}

// RecvLoop accept command from grpcStream and execute.
// RecvLoop is NON-repeatable function, one serverWatchStream can run RecvLoop only once
func (sws *serverWatchStream) RecvLoop() error {
	// RecvLoop done, recycle all watchers
	defer func() {
		func() {
			sws.Lock()
			defer sws.Unlock()

			for _, w := range sws.watcherBook {
				close(w.CtrlStream)
			}
			sws.watcherBook = map[int64]*mvcc.Watcher{}
		}()

		// wait all routine done
		sws.Wait()
		close(sws.eventStream)
	}()

	for {
		req, err := sws.grpcStream.Recv()
		if err != nil {
			return err
		}

		switch req.RequestUnion.(type) {
		case *pb.WatchRequest_CreateRequest:
			r := req.GetCreateRequest()

			w := &mvcc.Watcher{
				Store:         sws.store,
				Key:           r.Key,
				RangeEnd:      r.RangeEnd,
				StartRevision: uint64(r.StartRevision),
				CtrlStream:    make(chan *pb.WatchRequest, 1),
				EventStream:   make(chan *pb.WatchResponse, 1),
				WatchId:       uniqueID(),
			}

			// set watcher in watcherBook
			func() {
				sws.Lock()
				defer sws.Unlock()

				sws.watcherBook[w.WatchId] = w
			}()

			// go watch
			sws.Add(1)
			go func() {
				defer sws.Done()

				if err := w.Watch(); err != nil {
					log.Printf("w.Watch failed, err=%v\n", err)
				}
			}()

			// merge event stream in background
			sws.Add(1)
			go func() {
				defer sws.Done()

				for {
					e, ok := <-w.EventStream
					if !ok {
						break
					}

					sws.eventStream <- e
				}
			}()
		case *pb.WatchRequest_CancelRequest:
			r := req.GetCancelRequest()

			w := func() *mvcc.Watcher {
				sws.RLock()
				defer sws.RUnlock()

				w, ok := sws.watcherBook[r.WatchId]
				if !ok {
					return nil
				}

				return w
			}()
			if w == nil {
				return fmt.Errorf("get w failed, r=%v, sws.watcherBook=%v", r, sws.watcherBook)
			}

			w.CtrlStream <- req
		}
	}

	return nil
}

// SendLoop collect event from watchers and send back to grpcStream.
func (sws *serverWatchStream) SendLoop() error {
	for {
		e, ok := <-sws.eventStream
		if !ok {
			break
		}

		if e.Canceled {
			// get canceled watcher and delete from watcherBook
			w := func() *mvcc.Watcher {
				sws.Lock()
				defer sws.Unlock()

				w, ok := sws.watcherBook[e.WatchId]
				if !ok {
					return nil
				}
				delete(sws.watcherBook, e.WatchId)

				return w
			}()
			if w == nil {
				return fmt.Errorf("get w failed, e=%v, sws.watcherBook=%v", e, sws.watcherBook)
			}

			// close canceled watcher CtrlStream
			close(w.CtrlStream)
		}

		if err := sws.grpcStream.Send(e); err != nil {
			return err
		}
	}

	return nil
}

func uniqueID() int64 {
	// TODO: use real uniqueID
	return time.Now().UnixNano()
}
