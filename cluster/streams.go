package cluster

import (
	"context"

	pb "github.com/mdkhanga/dynago/kvmessages"
	"google.golang.org/grpc"
)

// Stream is a generic interface for both client and server streams
type IStream interface {
	Send(*pb.ServerMessage) error
	Receive() (*pb.ServerMessage, error)
	Context() context.Context
}

// ClientStream wraps grpc.ClientStream
type ClientStream struct {
	Stream grpc.BidiStreamingClient[pb.ServerMessage, pb.ServerMessage]
}

// Send sends a message on the client stream
func (c *ClientStream) Send(msg *pb.ServerMessage) error {
	return c.Stream.Send(msg)
}

// Receive receives a message from the client stream
func (c *ClientStream) Receive() (*pb.ServerMessage, error) {
	return c.Stream.Recv()
}

// Context retrieves metadata from the stream
func (c *ClientStream) Context() context.Context {
	// md, _ := metadata.FromOutgoingContext(c.Stream.Context())
	// return md

	return c.Stream.Context()
}

// ServerStream wraps grpc.ServerStream
type ServerStream struct {
	Stream pb.KVSevice_CommunicateServer
}

// Send sends a message on the server stream
func (s *ServerStream) Send(msg *pb.ServerMessage) error {
	return s.Stream.Send(msg)
}

// Receive receives a message from the server stream
func (s *ServerStream) Receive() (*pb.ServerMessage, error) {
	return s.Stream.Recv()
}

// Context retrieves metadata from the server stream
func (s *ServerStream) Context() context.Context {
	return s.Stream.Context()
}
