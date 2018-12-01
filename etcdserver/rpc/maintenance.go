package rpc

import (
	"context"
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/powerispower/TiDB-Hackathon2018/etcdserver"
)

type maintenanceServer struct {
	s *etcdserver.EtcdServer
}

func NewMaintenanceServer(s *etcdserver.EtcdServer) pb.MaintenanceServer {
	return &maintenanceServer{s: s}
}

func (s *maintenanceServer) Alarm(ctx context.Context, r *pb.AlarmRequest) (*pb.AlarmResponse, error) {
	return &pb.AlarmResponse{}, nil
}

func (s *maintenanceServer) Status(ctx context.Context, r *pb.StatusRequest) (*pb.StatusResponse, error) {
	return &pb.StatusResponse{Version: "3.2.1"}, nil
}

func (s *maintenanceServer) Defragment(ctx context.Context, r *pb.DefragmentRequest) (*pb.DefragmentResponse, error) {
	return &pb.DefragmentResponse{}, nil
}

func (s *maintenanceServer) Hash(ctx context.Context, r *pb.HashRequest) (*pb.HashResponse, error) {
	return &pb.HashResponse{}, nil
}

func (s *maintenanceServer) HashKV(ctx context.Context, r *pb.HashKVRequest) (*pb.HashKVResponse, error) {
	return &pb.HashKVResponse{}, nil
}

func (s *maintenanceServer) Snapshot(r *pb.SnapshotRequest, stream pb.Maintenance_SnapshotServer) error {
	stream.Send(&pb.SnapshotResponse{})
	return nil
}

func (s *maintenanceServer) MoveLeader(ctx context.Context, r *pb.MoveLeaderRequest) (*pb.MoveLeaderResponse, error) {
	return &pb.MoveLeaderResponse{}, nil
}
