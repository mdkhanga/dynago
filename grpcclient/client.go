package grpclient

import (
	"context"
	"sync"
	"time"

	"github.com/mdkhanga/dynago/cluster"
	pb "github.com/mdkhanga/dynago/kvmessages"
	"github.com/mdkhanga/dynago/logger"
	"github.com/mdkhanga/dynago/models"
	"github.com/mdkhanga/dynago/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	status "google.golang.org/grpc/status"
)

var (
	Log      = logger.WithComponent("grpcclient").Log
	stopChan = make(chan struct{})
	once     sync.Once
)

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

func Close() {
	once.Do(func() {
		Log.Info().Msg("client close called")
		if stopChan != nil {
			close(stopChan)
		}
	})
}

func CallGrpcServer(myhost *string, myport *int32, seedHostport *string) error {

	for {

		Log.Debug().Msg(" Calling grpc server")

		conn, err := grpc.NewClient(*seedHostport, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			Log.Error().AnErr("did not connect:", err).Send()
			Log.Info().Msg("Sleep for 5 sec and try again")
			time.Sleep(5 * time.Second)
			continue
		}
		defer conn.Close()

		c := pb.NewKVSeviceClient(conn)
		ctx := context.Background()
		// defer cancel()

		Log.Debug().Msg("Create KVclient")

		stream, err := c.Communicate(ctx)
		if err != nil {
			Log.Error().Msg("Error getting bidirectinal strem")
			conn.Close()
			Log.Info().Msg("Sleep for 5 sec and try again")
			time.Sleep(5 * time.Second)
			continue

		}

		sendMessageQueue := &MessageQueue{}
		receiveMessageQueue := &MessageQueue{}

		// stopChan := make(chan struct{})

		go sendLoop(stream, sendMessageQueue, stopChan)

		go receiveLoop(stream, receiveMessageQueue, stopChan)

		go pingLoop(sendMessageQueue, stopChan, myhost, myport)

		<-stopChan
		Log.Info().Msg("Stopping message processing due to stream error")
		stream.CloseSend()
		conn.Close()
		Log.Info().Msg("Sleep for 5 sec and try again")
		time.Sleep(5 * time.Second)

	}

}

func sendLoop(stream pb.KVSevice_CommunicateClient, messageQueue *MessageQueue, stopChan chan struct{}) {

	for {

		select {

		case <-stopChan:
			Log.Info().Msg("Stopping send goroutine ..")
			return

		default:
			msg := messageQueue.Dequeue()
			if msg == nil {
				time.Sleep(1 * time.Second) // Wait before checking again
				continue
			}

			// Log.Debug().Any("Dequed Sending message of type:", msg.Type)
			err := stream.Send(msg)
			if err != nil {
				Log.Error().AnErr("Error sending message: ", err)
				// close(stopChan)
				Close()
				return
			}

		}

		time.Sleep(5 * time.Second)

	}
}

func receiveLoop(stream pb.KVSevice_CommunicateClient, messageQueue *MessageQueue, stopChan chan struct{}) {

	for {
		msg, err := stream.Recv()
		if err != nil {

			code := status.Code(err)

			if code == codes.Unavailable || code == codes.Canceled || code == codes.DeadlineExceeded {

				Log.Error().Msg("Unable to read from the stream. server seems unavailable")
				// close(stopChan)
				Close()
				return

			}

		}
		// Log.Info().Any("Received message of type:", msg.Type).Send()

		if msg.Type == pb.MessageType_PING_RESPONSE {
			// Log.Info().Int32("Received Ping message from the stream ", msg.GetPingResponse().Hello)
		} else if msg.Type == pb.MessageType_CLUSTER_INFO_REQUEST {

			// Log.Info().Any("Recieved cluster member list", msg.GetClusterInfoRequest().GetCluster().Members).Send()
			cluster.ClusterService.MergePeerLists(msg.GetClusterInfoRequest().GetCluster().Members)
		} else if msg.Type == pb.MessageType_KEY_VALUE {
			Log.Info().Msg("received a key value msg")
			storage.Store.Set(&models.KeyValue{Key: msg.GetKeyValue().GetKey(), Value: msg.GetKeyValue().GetValue()})

		}

	}

}

func pingLoop(sendMessageQueue *MessageQueue, stopChan chan struct{}, myhost *string, myport *int32) {

	for {

		select {

		case <-stopChan:
			Log.Info().Msg("Stopping send goroutine ..")
			return

		default:

			msg := &pb.ServerMessage{
				Type: pb.MessageType_PING,
				Content: &pb.ServerMessage_Ping{
					Ping: &pb.PingRequest{Hello: 1, Hostname: *myhost, Port: *myport},
				},
			}

			sendMessageQueue.Enqueue(msg)

		}

		time.Sleep(1 * time.Second)

	}

}
