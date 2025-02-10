package cluster

import (
	"sync"
	"time"

	"github.com/mdkhanga/dynago/config"
	pb "github.com/mdkhanga/dynago/kvmessages"
	"github.com/mdkhanga/dynago/models"
	"github.com/mdkhanga/dynago/storage"
	"github.com/mdkhanga/dynago/utils"
)

type Peer struct {
	Host *string
	Port *int32
	// stream      pb.KVSevice_CommunicateServer
	stream      IStream
	InMessages  utils.MessageQueue
	OutMessages utils.MessageQueue
	Timestamp   int64
	Status      int  // 0 = Active, 1 = Inactive, 2 = unknown
	Mine        bool // true means peer is directly connected to me
	stopChan    chan struct{}
	once        sync.Once
	Clientend   bool // true = clientside of the peer false = server side of the peer
}

type IPeer interface {
	Init()
	ReceivedMessage(message *pb.ServerMessage)
	SendMessage(message *pb.ServerMessage)
	Stop()
}

func (p *Peer) ReceivedMessage(message *pb.ServerMessage) {

	p.InMessages.Enqueue(message)

}

func (p *Peer) SendMessage(message *pb.ServerMessage) {

	p.OutMessages.Enqueue(message)

}

func (p *Peer) close() {
	p.once.Do(func() {
		if p.stopChan != nil {
			close(p.stopChan)
		}
	})

}

func (p *Peer) Init() {

	p.stopChan = make(chan struct{})

	go p.receiveLoop()

	go p.processMessageLoop()

	go p.sendLoop()

	if p.Clientend == true {

		// start the ping loop
		Log.Info().Msg("Starting the ping loop")
		p.pingLoop()

	} else {
		Log.Info().Msg("Not Starting the ping loop")
	}

	<-p.stopChan
	Log.Info().Msg("Stopping message processing due to stream error")

}

func (p *Peer) Stop() {
	if p == nil {
		return
	}
	p.close()
}

// func NewPeer(s pb.KVSevice_CommunicateServer) IPeer {
func NewPeer(s IStream, client bool) IPeer {
	return &Peer{
		stream:      s,
		InMessages:  utils.MessageQueue{},
		OutMessages: utils.MessageQueue{},
		stopChan:    make(chan struct{}),
		once:        sync.Once{},
		Clientend:   client,
	}
}

func (p *Peer) receiveLoop() {

	ctx := p.stream.Context()

	for {

		select {

		case <-ctx.Done():
			Log.Info().Msg("Client disconnected or context canceled (receiver)")
			// closeStopChan()
			// p.close()
			p.Stop()
			return

		default:
			// in, err := stream.Recv()
			// in, err := p.stream.Recv()
			in, err := p.stream.Receive()
			if err != nil {

				// code := status.Code(err)

				// if code == codes.Unavailable || code == codes.Canceled || code == codes.DeadlineExceeded {

				Log.Info().Msg("Unable to read from the stream. server seems unavailable")
				// closeStopChan()
				// p.close()
				p.Stop()
				return
				// }
			}

			p.InMessages.Enqueue(in)

		}

	}

}

func (p *Peer) sendLoop() {

	ctx := p.stream.Context()

	for {
		select {
		case <-ctx.Done(): // Client disconnected or context canceled
			Log.Info().Msg("Client disconnected or context canceled (sender)")
			// closeStopChan()
			p.Stop()
			return
		case <-p.stopChan: // Stop signal received
			Log.Info().Msg("Stop signal received for sender goroutine")
			return
		default:
			// Send a message to the client (dummy example message)

			msg := p.OutMessages.Dequeue()
			if msg == nil {
				time.Sleep(1 * time.Second) // Wait before checking again
				continue
			}

			if err := p.stream.Send(msg); err != nil {
				Log.Error().AnErr("Error sending message:", err)

				// closeStopChan()
				p.Stop()
				return
			}

		}
	}
}

func (p *Peer) processMessageLoop() {

	for {

		select {

		case <-p.stopChan:
			Log.Info().Msg("Stop signal received for processing goroutine")
			return

		default:

			msg := p.InMessages.Dequeue()
			if msg == nil {
				time.Sleep(1 * time.Second) // Wait before checking again
				continue
			}

			var response *pb.ServerMessage
			switch msg.Type {
			case pb.MessageType_PING:

				host := msg.GetPing().Hostname
				port := msg.GetPing().Port

				response = &pb.ServerMessage{
					Type: pb.MessageType_PING_RESPONSE,
					Content: &pb.ServerMessage_PingResponse{
						PingResponse: &pb.PingResponse{Hello: 2},
					},
				}

				exists, _ := ClusterService.Exists(host, port)

				if !exists {

					p.Host = &host
					p.Port = &port
					p.Timestamp = time.Now().UnixMilli()
					p.Mine = false

					Log.Info().
						Str("Hostname", msg.GetPing().Hostname).
						Int32("port", msg.GetPing().Port).
						Msg("Adding to cluster")

					ClusterService.AddToCluster(p)
					Log.Info().Str("Hostname", host).
						Int32("Port", port).
						Msg("Added new server to Cluster")

				} else {
					// just update the timestamp

					p.Timestamp = time.Now().UnixMilli()
					if p.Port == nil {
						p.Host = &host
						p.Port = &port
						p.Mine = false
					}

					p.Status = 0
					ClusterService.AddToCluster(p)

				}
			case pb.MessageType_CLUSTER_INFO_REQUEST:
				diff := ClusterService.MergePeerLists(msg.GetClusterInfoRequest().GetCluster().Members)
				cls := pb.Cluster{Members: diff}

				clsResp := pb.ClusterInfoResponse{Status: pb.ClusterInfoResponse_NEED_UPDATES, Cluster: &cls}

				clsServerMsg := pb.ServerMessage{
					Type:    pb.MessageType_CLUSTER_INFO_RESPONSE,
					Content: &pb.ServerMessage_ClusterInfoReponse{ClusterInfoReponse: &clsResp}}

				p.OutMessages.Enqueue(&clsServerMsg)
			case pb.MessageType_CLUSTER_INFO_RESPONSE:
				ClusterService.MergePeerLists(msg.GetClusterInfoReponse().GetCluster().Members)

			case pb.MessageType_KEY_VALUE:
				Log.Info().Msg("Received KeyValueMessage")
				key := msg.GetKeyValue().GetKey()
				val := msg.GetKeyValue().GetValue()
				Log.Info().Str("Key=", key).Str("value=", val).Send()
				storage.Store.Set(&models.KeyValue{Key: msg.GetKeyValue().GetKey(), Value: msg.GetKeyValue().GetValue()})
				// Handle KeyValueMessage
			case pb.MessageType_PING_RESPONSE:
				// no op for now
			default:
				Log.Info().Msg("Unknown message type received")
			}

			p.OutMessages.Enqueue(response)

		}

	}

}

func (p *Peer) pingLoop() {

	cfg := config.GetConfig()
	ctx := p.stream.Context()

	for {

		select {

		case <-ctx.Done(): // Client disconnected or context canceled
			Log.Info().Msg("Client disconnected or context canceled (sender)")
			// closeStopChan()
			p.Stop()
			return
		case <-p.stopChan: // Stop signal received
			Log.Info().Msg("Stop signal received for sender goroutine")
			return

		default:

			msg := &pb.ServerMessage{
				Type: pb.MessageType_PING,
				Content: &pb.ServerMessage_Ping{
					Ping: &pb.PingRequest{Hello: 1, Hostname: cfg.Hostname, Port: cfg.GrpcPort},
				},
			}

			p.OutMessages.Enqueue(msg)

		}

		time.Sleep(1 * time.Second)

	}

}
