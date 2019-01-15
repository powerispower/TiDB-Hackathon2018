package rpc

import (
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/powerispower/TiDB-Hackathon2018/etcdserver"
	"google.golang.org/grpc"
	"net"

	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
)

func launchMockServer() (*grpc.Server, net.Addr, error) {
	// open store
	var driver mockstore.MockDriver
	store, err := driver.Open("mocktikv://")
	if err != nil {
		return nil, nil, err
	}
	etcdServer := etcdserver.NewServer(store)

	grpcServer := grpc.NewServer()
	pb.RegisterKVServer(grpcServer, NewKVServer(etcdServer))
	pb.RegisterMaintenanceServer(grpcServer, NewMaintenanceServer(etcdServer))
	pb.RegisterClusterServer(grpcServer, NewClusterServer(etcdServer))
	pb.RegisterWatchServer(grpcServer, NewWatchServer(etcdServer))

	lis, err := net.Listen("tcp", "[::1]:0")
	if err != nil {
		return nil, nil, err
	}

	go func() {
		grpcServer.Serve(lis)
	}()

	return grpcServer, lis.Addr(), nil
}
