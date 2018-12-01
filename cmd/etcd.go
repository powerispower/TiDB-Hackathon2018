package main

import (
	"flag"
	"fmt"
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/powerispower/TiDB-Hackathon2018/etcdserver"
	"github.com/powerispower/TiDB-Hackathon2018/etcdserver/rpc"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
)

var (
	clientURL = flag.String("listen-client-url", "0.0.0.0:2379", "listen client url")
	pdURL     = flag.String("pd-url", "127.0.0.1:3379", "tikv pd url")
)

func main() {
	flag.Parse()
	driver := tikv.Driver{}
	store, err := driver.Open(fmt.Sprintf("tikv://%s", *pdURL))
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	etcdServer := etcdserver.NewServer(store)

	lis, err := net.Listen("tcp", *clientURL)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterKVServer(grpcServer, rpc.NewKVServer(etcdServer))
	pb.RegisterMaintenanceServer(grpcServer, rpc.NewMaintenanceServer(etcdServer))
	grpcServer.Serve(lis)
}
