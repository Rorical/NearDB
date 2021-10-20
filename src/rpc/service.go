package rpc

import (
	"context"
	"github.com/Rorical/NearDB/src/database"
	"github.com/Rorical/NearDB/src/rpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
)

type NearDBService struct {
	DB *database.NearDBDatabase
}

func (s *NearDBService) Add(ctx context.Context, in *pb.AddRequest) (*pb.NoneResponse, error) {
	return &pb.NoneResponse{}, s.DB.Add(in.Id, in.Taglist)
}

func (s *NearDBService) Remove(ctx context.Context, in *pb.AddRequest) (*pb.NoneResponse, error) {
	return &pb.NoneResponse{}, s.DB.Remove(in.Id)
}

func (s *NearDBService) Query(ctx context.Context, in *pb.QueryRequest) (*pb.QueryResponse, error) {
	items, err := s.DB.Query(in.Taglist, int(in.K))
	resitems := make([]*pb.Item, len(items))
	for i, item := range items {
		resitems[i] = &pb.Item{
			Id:       item.Id,
			Distance: item.Distance,
		}
	}
	return &pb.QueryResponse{
		Items: resitems,
	}, err
}

func NewService() (*NearDBService, error) {
	db, err := database.NewDatabase()
	if err != nil {
		return nil, err
	}
	db.Refresh()
	return &NearDBService{
		DB: db,
	}, nil
}

func RunService(uri string, ser *NearDBService) {
	var err error
	listen, err := net.Listen("tcp", uri)
	if err != nil {
		panic(err)
	}
	server := grpc.NewServer()
	pb.RegisterNearDBServiceServer(server, ser)
	reflection.Register(server)
	err = server.Serve(listen)
	if err != nil {
		panic(err)
	}
	defer ser.DB.Close()
}
