package cluster

import (
	"sync"
	"time"

	"github.com/mdkhanga/dynago/config"
	pb "github.com/mdkhanga/dynago/kvmessages"
	"github.com/mdkhanga/dynago/models"
	"github.com/mdkhanga/dynago/storage"
)

type Peer struct {
	Host      *string
	Port      *int32
	stream    IStream
	Timestamp int64
	Status    int  // 0 = Active, 1 = Inactive, 2 = unknown
	Mine      bool // true means peer is directly connected to me
	stopChan  chan struct{}
	once      sync.Once
	Clientend bool // true = clientside of the peer false = server side of the peer
	mu        sync.Mutex

	// rewrite in thread safe way
	InMessagesChan  chan *pb.ServerMessage // Buffered channel for inbound messages
	OutMessagesChan chan *pb.ServerMessage
}

type IPeer interface {
	Init()
	Stop()
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

	go p.receiveLoopWithChannel()

	go p.processMessageLoopWithChannel()

	go p.sendLoopWithChannel()

	if p.Clientend != true {

		Log.Info().Msg("Starting the ping loop")
		go p.pingLoopWithChannel()

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

func NewPeer(s IStream, client bool) IPeer {
	return &Peer{
		stream:    s,
		stopChan:  make(chan struct{}),
		once:      sync.Once{},
		Clientend: client,

		InMessagesChan:  make(chan *pb.ServerMessage, 100),
		OutMessagesChan: make(chan *pb.ServerMessage, 100),
	}
}

func (p *Peer) receiveLoopWithChannel() {

	ctx := p.stream.Context()

	count := 0

	for {

		if p.Status == 1 {
			return
		}

		select {

		case <-ctx.Done():
			Log.Info().Msg("Client disconnected or context canceled (receiver)")
			p.Stop()
			return
		case <-p.stopChan:
			Log.Info().Msg("Stop signal received for sender goroutine")
			return

		default:

			in, err := p.stream.Receive()
			if err != nil {

				Log.Info().Msg("Unable to read from the stream. server seems unavailable")

				p.Stop()
				return
				// }
			}

			count++

			p.InMessagesChan <- in

		}

	}

}

func (p *Peer) sendLoopWithChannel() {

	ctx := p.stream.Context()

	count := 00

	for {

		if p.Status == 1 {
			Log.Info().Msg("Peer is inactive. Exit send loop")
			return
		}

		select {
		case <-ctx.Done(): // Client disconnected or context canceled
			Log.Info().Msg("Client disconnected or context canceled (sender)")
			// closeStopChan()
			p.Stop()
			return
		case <-p.stopChan: // Stop signal received
			Log.Info().Msg("Stop signal received for sender goroutine")
			return
		case msg, _ := <-p.OutMessagesChan: // Properly read from channel

			count++
			// Log.Info().Int("sending msg", count).Send()

			if err := p.stream.Send(msg); err != nil {
				Log.Error().AnErr("Error sending message:", err)
				p.Stop()
				return
			}
		}
	}
}

func (p *Peer) processMessageLoopWithChannel() {

	count := 0

	for {

		if p.Status == 1 {
			return
		}

		select {

		case <-p.stopChan:
			Log.Info().Msg("Stop signal received for processing goroutine")
			return

		case msg := <-p.InMessagesChan:

			if msg == nil {
				time.Sleep(1 * time.Second) // Wait before checking again
				continue
			}

			switch msg.Type {
			case pb.MessageType_PING:

				count++

				host := msg.GetPing().GetHostname()
				port := msg.GetPing().GetPort()

				response := &pb.ServerMessage{
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

				p.OutMessagesChan <- response

			case pb.MessageType_CLUSTER_INFO_REQUEST:

				diff := ClusterService.MergePeerLists(msg.GetClusterInfoRequest().GetCluster().GetMembers(), true)

				cls := pb.Cluster{Members: diff}

				clsResp := pb.ClusterInfoResponse{Status: pb.ClusterInfoResponse_NEED_UPDATES, Cluster: &cls}

				clsServerMsg := pb.ServerMessage{
					Type:    pb.MessageType_CLUSTER_INFO_RESPONSE,
					Content: &pb.ServerMessage_ClusterInfoReponse{ClusterInfoReponse: &clsResp}}

				p.OutMessagesChan <- &clsServerMsg

			case pb.MessageType_CLUSTER_INFO_RESPONSE:
				ClusterService.MergePeerLists(msg.GetClusterInfoReponse().GetCluster().GetMembers(), false)

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

		}

	}

}

func (p *Peer) pingLoopWithChannel() {

	cfg := config.GetConfig()
	ctx := p.stream.Context()

	count := 0

	for {

		if p.Status == 1 {
			return
		}

		select {

		case <-ctx.Done(): // Client disconnected or context canceled
			Log.Info().Msg("Client disconnected or context canceled (sender)")
			// closeStopChan()
			p.Stop()
			return
		case <-p.stopChan: // Stop signal received
			Log.Info().Msg("Stop signal received for sender goroutine")
			return

		case <-time.After(1 * time.Second): // Send PING at regular intervals
			msg := &pb.ServerMessage{
				Type: pb.MessageType_PING,
				Content: &pb.ServerMessage_Ping{
					Ping: &pb.PingRequest{Hello: 1, Hostname: cfg.Hostname, Port: cfg.GrpcPort},
				},
			}

			count++
			p.OutMessagesChan <- msg
		}

	}

}
