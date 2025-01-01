package grpcserver

import (
	"context"
	"fmt"

	"net"
	"sync"

	"github.com/mdkhanga/kvstore/cluster"
	pb "github.com/mdkhanga/kvstore/kvmessages"
	"github.com/mdkhanga/kvstore/logger"
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

// Queue to hold incoming messages
type MessageQueue struct {
	messages []*pb.ServerMessage
	mu       sync.Mutex
}

// Enqueue adds a message to the queue
func (q *MessageQueue) Enqueue(msg *pb.ServerMessage) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.messages = append(q.messages, msg)
}

// Dequeue removes and returns the oldest message from the queue
func (q *MessageQueue) Dequeue() *pb.ServerMessage {

	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.messages) == 0 {
		return nil
	}
	msg := q.messages[0]
	q.messages = q.messages[1:]
	return msg
}

func (s *Server) Communicate(stream pb.KVSevice_CommunicateServer) error {

	Log.Info().Msg("Server received a request to connect")

	p := cluster.NewPeer(stream)

	p.Init()

	// Log.Info().Msg("Established a peer ")

	return nil
}

func StartGrpcServer(hostPtr *string, portPtr *string) {

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%s", *hostPtr, *portPtr))
	// lis, err := net.Listen("tcp", fmt.Sprintf("192.168.1.15:%s", *portPtr))
	if err != nil {
		Log.Error().AnErr("failed to listen:", err).Send()
	}

	s := grpc.NewServer()
	pb.RegisterKVSeviceServer(s, &Server{})
	Log.Info().Any("GRPC server listening at ", lis.Addr().String()).Send()
	if err := s.Serve(lis); err != nil {
		Log.Error().AnErr("failed to serve: ", err).Send()
	}

}
