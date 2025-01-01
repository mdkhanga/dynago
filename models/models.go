package models

import (
	pb "github.com/mdkhanga/dynago/kvmessages"
	"github.com/mdkhanga/dynago/utils"
)

type KeyValue struct {
	Key   string
	Value string
}

type ClusterMember struct {
	Host string
	Port int32
}

type Peer struct {
	Host        *string
	Port        *int32
	stream      pb.KVSevice_CommunicateServer
	inMessages  utils.MessageQueue
	outMessages utils.MessageQueue
}
