package cluster

import (
	pb "github.com/mdkhanga/dynago/kvmessages"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// Stream is a generic interface for both client and server streams
type Stream interface {
	Send(*pb.ServerMessage) error
	Receive() (*pb.ServerMessage, error)
	Context() metadata.MD
}

// ClientStream wraps grpc.ClientStream
type ClientStream struct {
	stream grpc.BidiStreamingClient[pb.ServerMessage, pb.ServerMessage]
}

// Send sends a message on the client stream
func (c *ClientStream) Send(msg *pb.ServerMessage) error {
	return c.stream.Send(msg)
}

// Receive receives a message from the client stream
func (c *ClientStream) Receive() (*pb.ServerMessage, error) {
	return c.stream.Recv()
}

// Context retrieves metadata from the stream
func (c *ClientStream) Context() metadata.MD {
	md, _ := metadata.FromOutgoingContext(c.stream.Context())
	return md
}

// ServerStream wraps grpc.ServerStream
type ServerStream struct {
	stream pb.KVSevice_CommunicateServer
}

// Send sends a message on the server stream
func (s *ServerStream) Send(msg *pb.ServerMessage) error {
	return s.stream.Send(msg)
}

// Receive receives a message from the server stream
func (s *ServerStream) Receive() (*pb.ServerMessage, error) {
	return s.stream.Recv()
}

// Context retrieves metadata from the server stream
func (s *ServerStream) Context() metadata.MD {
	md, _ := metadata.FromIncomingContext(s.stream.Context())
	return md
}
