package etcdserver

import (
	"github.com/pingcap/tidb/kv"
)

type EtcdServer struct {
	Store kv.Storage
}

func NewServer(store kv.Storage) *EtcdServer {
	return &EtcdServer{Store: store}
}
