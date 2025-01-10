package handlers

import (
	"github.com/mdkhanga/dynago/cluster"
	pb "github.com/mdkhanga/dynago/kvmessages"
)

func PingHandler(msg *pb.ServerMessage, p *cluster.Peer) *pb.ServerMessage {

	host := msg.GetPing().Hostname
	port := msg.GetPing().Port

	Log.Info().Int32("hello", msg.GetPing().Hello).
		Str("Hostname", msg.GetPing().Hostname).
		Int32("port", msg.GetPing().Port).
		Msg("Received Ping message from the stream")

	response := &pb.ServerMessage{
		Type: pb.MessageType_PING_RESPONSE,
		Content: &pb.ServerMessage_PingResponse{
			PingResponse: &pb.PingResponse{Hello: 2},
		},
	}

	if exists, _ := cluster.ClusterService.Exists(host, port); !exists {

		p.Host = &host
		p.Port = &port

		Log.Info().
			Str("Hostname", msg.GetPing().Hostname).
			Int32("port", msg.GetPing().Port).
			Msg("Adding to cluster")

		cluster.ClusterService.AddToCluster(p)
		Log.Info().Str("Hostname", host).
			Int32("Port", port).
			Msg("Added new server to Cluster")
	}

	return response
}
