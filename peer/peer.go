package peer

import (
	pb "github.com/mdkhanga/kvstore/kvmessages"
	"github.com/mdkhanga/kvstore/utils"
)

type Peer struct {
	Host        *string
	Port        *int32
	stream      pb.KVSevice_CommunicateServer
	inMessages  utils.MessageQueue
	outMessages utils.MessageQueue
}

type IPeer interface {
	ReceivedMessage(message *pb.ServerMessage)
	SendMessage(message *pb.ServerMessage)
}

func (p *Peer) ReceivedMessage(message *pb.ServerMessage) {

}

func (p *Peer) SendMessage(message *pb.ServerMessage) {

}

func New(host *string, port *int32, s pb.KVSevice_CommunicateServer) IPeer {

	return &Peer{
		Host:        host,
		Port:        port,
		stream:      s,
		inMessages:  utils.MessageQueue{},
		outMessages: utils.MessageQueue{},
	}
}
