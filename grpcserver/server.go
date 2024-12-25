package grpcserver

import (
	"context"
	"fmt"

	"net"
	"sync"
	"time"

	"github.com/mdkhanga/kvstore/cluster"
	pb "github.com/mdkhanga/kvstore/kvmessages"
	"github.com/mdkhanga/kvstore/logger"
	"github.com/mdkhanga/kvstore/models"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	peer "google.golang.org/grpc/peer"
	status "google.golang.org/grpc/status"
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

	sendMessageQueue := &MessageQueue{}
	receiveMessageQueue := &MessageQueue{}

	stopChan := make(chan struct{})

	var once sync.Once

	// Function to safely close the stopChan
	closeStopChan := func() {
		once.Do(func() {
			close(stopChan)
		})
	}

	go receiveLoop(stream, receiveMessageQueue, stopChan, closeStopChan)

	go processMessageLoop(&stream, receiveMessageQueue, sendMessageQueue, stopChan, closeStopChan)

	go sendLoop(stream, sendMessageQueue, stopChan, closeStopChan)

	<-stopChan
	Log.Info().Msg("Stopping message processing due to stream error")

	return nil

	// Block main goroutine to keep stream open
	// select {}
}

func StartGrpcServer(portPtr *string) {

	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", *portPtr))
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

func receiveLoop(stream pb.KVSevice_CommunicateServer, messageQueue *MessageQueue, stopChan chan struct{}, closeStopChan func()) {

	ctx := stream.Context()

	for {

		select {

		case <-ctx.Done():
			Log.Info().Msg("Client disconnected or context canceled (receiver)")
			// close(stopChan)
			closeStopChan()
			return

		default:
			in, err := stream.Recv()
			if err != nil {

				code := status.Code(err)

				if code == codes.Unavailable || code == codes.Canceled || code == codes.DeadlineExceeded {

					Log.Info().Msg("Unable to read from the stream. server seems unavailable")
					closeStopChan()
					return
				}
			}

			Log.Info().Any("Received message of type:", in.Type).Send()
			if in.Type == pb.MessageType_PING {
				Log.Info().Int32("hello", in.GetPing().Hello).
					Str("Hostname", in.GetPing().Hostname).
					Int32("port", in.GetPing().Port).
					Msg("Received Ping message from the stream")

				messageQueue.Enqueue(in)
				Log.Info().Int("Server Queue length", len(messageQueue.messages)).Send()
			}

		}

	}

}

func sendLoop(stream pb.KVSevice_CommunicateServer, messageQueue *MessageQueue, stopChan chan struct{}, closeStopChan func()) {

	ctx := stream.Context()

	for {
		select {
		case <-ctx.Done(): // Client disconnected or context canceled
			Log.Info().Msg("Client disconnected or context canceled (sender)")
			// close(stopChan)
			closeStopChan()
			return
		case <-stopChan: // Stop signal received
			Log.Info().Msg("Stop signal received for sender goroutine")
			return
		default:
			// Send a message to the client (dummy example message)

			msg := messageQueue.Dequeue()
			if msg == nil {
				time.Sleep(1 * time.Second) // Wait before checking again
				continue
			}

			if err := stream.Send(msg); err != nil {
				Log.Error().AnErr("Error sending message:", err)

				closeStopChan()
				return
			}

		}
	}
}

func processMessageLoop(stream *pb.KVSevice_CommunicateServer, receiveMessageQueue *MessageQueue, sendMessageQueue *MessageQueue, stopChan chan struct{}, closeStopChan func()) {

	for {

		select {

		case <-stopChan:
			Log.Info().Msg("Stop signal received for processing goroutine")
			return

		default:

			msg := receiveMessageQueue.Dequeue()
			if msg == nil {
				time.Sleep(1 * time.Second) // Wait before checking again
				continue
			}

			var response *pb.ServerMessage
			switch msg.Type {
			case pb.MessageType_PING:

				host := msg.GetPing().Hostname
				port := msg.GetPing().Port

				Log.Info().Int32("hello", msg.GetPing().Hello).
					Str("Hostname", msg.GetPing().Hostname).
					Int32("port", msg.GetPing().Port).
					Msg("Received Ping message from the stream")

				response = &pb.ServerMessage{
					Type: pb.MessageType_PING_RESPONSE,
					Content: &pb.ServerMessage_PingResponse{
						PingResponse: &pb.PingResponse{Hello: 2},
					},
				}

				if exists, _ := cluster.ClusterService.Exists(host, port); !exists {
					cluster.ClusterService.AddToCluster(&models.ClusterMember{Host: host, Port: port, Stream: stream})
					Log.Info().Str("Hostname", host).
						Int32("Port", port).
						Msg("Added new server to Cluster")

				}

			case pb.MessageType_KEY_VALUE:
				Log.Info().Msg("Processing KeyValueMessage")
				// Handle KeyValueMessage
			default:
				Log.Info().Msg("Unknown message type received")
			}

			sendMessageQueue.Enqueue(response)

		}

	}

}
