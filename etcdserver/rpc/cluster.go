package rpc

import (
	"context"
	"github.com/powerispower/TiDB-Hackathon2018/etcdserver"

	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
)

type clusterServer struct {
	s *etcdserver.EtcdServer
}

func NewClusterServer(s *etcdserver.EtcdServer) pb.ClusterServer {
	return &clusterServer{s: s}
}

func (s *clusterServer) MemberAdd(context.Context, *pb.MemberAddRequest) (*pb.MemberAddResponse, error) {
	return &pb.MemberAddResponse{}, nil
}

func (s *clusterServer) MemberRemove(context.Context, *pb.MemberRemoveRequest) (*pb.MemberRemoveResponse, error) {
	return &pb.MemberRemoveResponse{}, nil
}

func (s *clusterServer) MemberUpdate(context.Context, *pb.MemberUpdateRequest) (*pb.MemberUpdateResponse, error) {
	return &pb.MemberUpdateResponse{}, nil
}

func (s *clusterServer) MemberList(context.Context, *pb.MemberListRequest) (*pb.MemberListResponse, error) {
	return &pb.MemberListResponse{
		Members: []*pb.Member{
			&pb.Member{
				ID:       0,
				Name:     "ti_etcd",
				PeerURLs: []string{},
				ClientURLs: []string{
					"127.0.0.1:2379",
				},
			},
		},
	}, nil
}
