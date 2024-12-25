package models

import (
	pb "github.com/mdkhanga/kvstore/kvmessages"
)

type KeyValue struct {
	Key   string
	Value string
}

type ClusterMember struct {
	Host   string
	Port   int32
	Stream *pb.KVSevice_CommunicateServer
}
