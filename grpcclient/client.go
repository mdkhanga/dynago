package grpclient

import (
	"context"
	"time"

	"github.com/mdkhanga/dynago/cluster"
	pb "github.com/mdkhanga/dynago/kvmessages"
	"github.com/mdkhanga/dynago/logger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	Log = logger.WithComponent("grpcclient").Log
	// stopChan = make(chan struct{})
	// once     sync.Once
)

/*
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
*/
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

		p := cluster.NewPeer(&cluster.ClientStream{Stream: stream}, true)

		p.Init()

		// <-stopChan
		Log.Info().Msg("Stopping message processing due to stream error")
		stream.CloseSend()
		conn.Close()
		Log.Info().Msg("Sleep for 5 sec and try again")
		time.Sleep(5 * time.Second)

	}

}
