package cluster

import (
	"sync"
	"time"

	pb "github.com/mdkhanga/dynago/kvmessages"
	"github.com/mdkhanga/dynago/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Peer struct {
	Host        *string
	Port        *int32
	stream      pb.KVSevice_CommunicateServer
	InMessages  utils.MessageQueue
	OutMessages utils.MessageQueue
	Timestamp   int64
	Status      int  // 0 = Active, 1 = Inactive, 2 = unknown
	Mine        bool // true means peer is directly connected to me
}

type IPeer interface {
	Init()
	ReceivedMessage(message *pb.ServerMessage)
	SendMessage(message *pb.ServerMessage)
}

func (p *Peer) ReceivedMessage(message *pb.ServerMessage) {

	p.InMessages.Enqueue(message)

}

func (p *Peer) SendMessage(message *pb.ServerMessage) {

	p.OutMessages.Enqueue(message)

}

func (p *Peer) Init() {

	stopChan := make(chan struct{})

	var once sync.Once

	// Function to safely close the stopChan
	closeStopChan := func() {
		once.Do(func() {
			close(stopChan)
		})

		ClusterService.RemoveFromCluster(*p.Host, *p.Port)
	}

	go p.receiveLoop(stopChan, closeStopChan)

	go p.processMessageLoop(stopChan, closeStopChan)

	go p.sendLoop(stopChan, closeStopChan)

	<-stopChan
	Log.Info().Msg("Stopping message processing due to stream error")

}

func NewPeer(s pb.KVSevice_CommunicateServer) IPeer {

	return &Peer{
		stream:      s,
		InMessages:  utils.MessageQueue{},
		OutMessages: utils.MessageQueue{},
	}
}

func (p *Peer) receiveLoop(stopChan chan struct{}, closeStopChan func()) {

	ctx := p.stream.Context()

	for {

		select {

		case <-ctx.Done():
			Log.Info().Msg("Client disconnected or context canceled (receiver)")
			// close(stopChan)
			closeStopChan()
			return

		default:
			// in, err := stream.Recv()
			in, err := p.stream.Recv()
			if err != nil {

				code := status.Code(err)

				if code == codes.Unavailable || code == codes.Canceled || code == codes.DeadlineExceeded {

					Log.Info().Msg("Unable to read from the stream. server seems unavailable")
					closeStopChan()
					return
				}
			}

			// Log.Info().Any("Received message of type:", in.Type).Send()
			if in.Type == pb.MessageType_PING {
				Log.Info().Int32("hello", in.GetPing().Hello).
					Str("Hostname", in.GetPing().Hostname).
					Int32("port", in.GetPing().Port).
					Msg("Received Ping message from the stream")

				p.InMessages.Enqueue(in)
				// Log.Info().Int("Server Queue length", p.inMessages.Length()).Send()
			}

		}

	}

}

func (p *Peer) sendLoop(stopChan chan struct{}, closeStopChan func()) {

	ctx := p.stream.Context()

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

			msg := p.OutMessages.Dequeue()
			if msg == nil {
				time.Sleep(1 * time.Second) // Wait before checking again
				continue
			}

			if err := p.stream.Send(msg); err != nil {
				Log.Error().AnErr("Error sending message:", err)

				closeStopChan()
				return
			}

		}
	}
}

func (p *Peer) processMessageLoop(stopChan chan struct{}, closeStopChan func()) {

	for {

		select {

		case <-stopChan:
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

				if exists, _ := ClusterService.Exists(host, port); !exists {

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
					p.Status = 0
				}

			case pb.MessageType_KEY_VALUE:
				Log.Info().Msg("Processing KeyValueMessage")
				// Handle KeyValueMessage
			default:
				Log.Info().Msg("Unknown message type received")
			}

			p.OutMessages.Enqueue(response)

		}

	}

}
