package cluster

import (
	"sync"
	"time"

	"github.com/mdkhanga/dynago/config"
	pb "github.com/mdkhanga/dynago/kvmessages"
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
		// Log.Info().Int32("P close called", *p.Port).Send()
		if p.stopChan != nil {
			close(p.stopChan)
		}
	})

}

func (p *Peer) Init() {

	p.stopChan = make(chan struct{})

	var once sync.Once

	// Function to safely close the stopChan
	closeStopChan := func() {
		once.Do(func() {
			close(p.stopChan)
			Log.Info().Msg("old close called")
		})

		ClusterService.RemoveFromCluster(*p.Host, *p.Port)
	}

	go p.receiveLoop(closeStopChan)

	go p.processMessageLoop(closeStopChan)

	go p.sendLoop(closeStopChan)

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
	// Log.Info().Int32("port", *p.Port).Bool("me", p.Mine).Send()
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

func (p *Peer) receiveLoop(closeStopChan func()) {

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

			// Log.Info().Any("Received message of type:", in.Type).Send()
			if in.Type == pb.MessageType_PING {
				Log.Info().Int32("Received a ping message", in.GetPing().Hello).
					Str("Hostname", in.GetPing().Hostname).
					Int32("port", in.GetPing().Port).
					Msg("Received Ping message from the stream")

				p.InMessages.Enqueue(in)
				// Log.Info().Int("Server Queue length", p.inMessages.Length()).Send()
			} else if in.Type == pb.MessageType_CLUSTER_INFO_REQUEST {

				// Log.Info().Any("Recieved cluster member list", msg.GetClusterInfoRequest().GetCluster().Members).Send()
				ClusterService.MergePeerLists(in.GetClusterInfoRequest().GetCluster().Members)
			}

		}

	}

}

func (p *Peer) sendLoop(closeStopChan func()) {

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

func (p *Peer) processMessageLoop(closeStopChan func()) {

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

			case pb.MessageType_KEY_VALUE:
				Log.Info().Msg("Received KeyValueMessage")
				key := msg.GetKeyValue().GetKey()
				val := msg.GetKeyValue().GetValue()
				Log.Info().Str("Key=", key).Str("value=", val).Send()
				// Handle KeyValueMessage
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
