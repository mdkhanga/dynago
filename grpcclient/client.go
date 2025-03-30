package grpclient

import (
	"context"
	"time"

	"github.com/mdkhanga/dynago/cluster"
	pb "github.com/mdkhanga/dynago/kvmessages"
	"github.com/mdkhanga/dynago/logger"
	"github.com/mdkhanga/dynago/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	Log = logger.WithComponent("grpcclient").Log
)

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

		Log.Debug().Msg("Create KVclient")

		stream, err := c.Communicate(ctx)
		if err != nil {
			Log.Error().Msg("Error getting bidirectinal strem")
			conn.Close()
			Log.Info().Msg("Sleep for 5 sec and try again")
			time.Sleep(5 * time.Second)
			continue

		}

		p := cluster.NewPeer(&cluster.ClientStream{Stream: stream}, false)

		host, port, _ := utils.ParseHostPort(*seedHostport)

		p.Host = &host
		p.Port = &port
		cluster.ClusterService.AddToCluster(p)
		p.Init()

		// <-stopChan
		Log.Info().Msg("Stopping message processing due to stream error")
		stream.CloseSend()
		conn.Close()
		Log.Info().Msg("Sleep for 5 sec and try again")
		time.Sleep(5 * time.Second)

	}

}

func CallPeer(myhost *string, myport *int32, seedHostport *string, p cluster.IPeer) (grpc.BidiStreamingClient[pb.ServerMessage, pb.ServerMessage], error) {

	conn, err := grpc.NewClient(*seedHostport, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		Log.Error().AnErr("did not connect:", err).Send()
		return nil, err
	}
	defer conn.Close()

	c := pb.NewKVSeviceClient(conn)
	ctx := context.Background()

	Log.Debug().Msg("Create KVclient")

	stream, err := c.Communicate(ctx)
	if err != nil {
		Log.Error().Msg("Error getting bidirectinal strem")
		conn.Close()
		return nil, err

	}

	return stream, nil

}
