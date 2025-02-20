package grpcserver

import (
	"context"
	"fmt"

	"net"
	"sync"

	"github.com/mdkhanga/dynago/cluster"
	pb "github.com/mdkhanga/dynago/kvmessages"
	"github.com/mdkhanga/dynago/logger"
	"google.golang.org/grpc"
	peer "google.golang.org/grpc/peer"
)

// server is used to implement helloworld.GreeterServer.
type Server struct {
	pb.UnimplementedKVSeviceServer
}

var (
	Log = logger.WithComponent("grpcserver").Log
)

var grpcserver *grpc.Server

// SayHello implements helloworld.GreeterServer
func (s *Server) Ping(ctx context.Context, in *pb.PingRequest) (*pb.PingResponse, error) {
	Log.Info().Int32("Received", in.GetHello()).Send()
	peerInfo, ok := peer.FromContext(ctx)
	if ok {
		fmt.Println("Client Address", peerInfo.Addr.String())
		fmt.Println("Client Address", peerInfo.LocalAddr.String())
	}
	return &pb.PingResponse{Hello: in.GetHello()}, nil
}

type MessageQueue struct {
	messages []*pb.ServerMessage
	mu       sync.Mutex
}

func (s *Server) Communicate(stream pb.KVSevice_CommunicateServer) error {

	Log.Info().Msg("Server received a request to connect")

	// p := cluster.NewPeer(s)
	p := cluster.NewPeer(&cluster.ServerStream{Stream: stream}, false)

	p.Init()

	// Log.Info().Msg("Established a peer ")

	return nil
}

func StartGrpcServer(hostPtr *string, portPtr *int32) {

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *hostPtr, *portPtr))
	// lis, err := net.Listen("tcp", fmt.Sprintf("192.168.1.15:%s", *portPtr))
	if err != nil {
		Log.Error().AnErr("failed to listen:", err).Send()
	}

	grpcserver = grpc.NewServer()
	pb.RegisterKVSeviceServer(grpcserver, &Server{})
	Log.Info().Any("GRPC server listening at ", lis.Addr().String()).Send()
	if err := grpcserver.Serve(lis); err != nil {
		Log.Error().AnErr("failed to serve: ", err).Send()
	}

}

func StopGrpcServer() {

	if grpcserver != nil {
		Log.Info().Msg("Stopping gRPC server...")
		grpcserver.Stop()
		// grpcserver.GracefulStop() // Gracefully stop the server
		Log.Info().Msg("gRPC server stopped.")
	} else {
		Log.Warn().Msg("gRPC server is not running.")
	}

}
