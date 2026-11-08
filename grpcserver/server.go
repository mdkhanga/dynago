package grpcserver

import (
	"context"
	"fmt"

	"github.com/mdkhanga/dynago/cluster"
	pb "github.com/mdkhanga/dynago/kvmessages"
	"github.com/mdkhanga/dynago/logger"
	peer "google.golang.org/grpc/peer"
)

// server is used to implement helloworld.GreeterServer.
type Server struct {
	pb.UnimplementedKVSeviceServer
}

var (
	Log = logger.WithComponent("grpcserver").Log
)

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

func (s *Server) Communicate(stream pb.KVSevice_CommunicateServer) error {

	Log.Info().Msg("Server received a request to connect")

	// p := cluster.NewPeer(s)
	p := cluster.NewPeer(&cluster.ServerStream{Stream: stream}, true)

	p.Init()

	// Log.Info().Msg("Established a peer ")

	return nil
}
