package cluster

import (
	"fmt"
	"strings"
	"sync"

	"time"

	"github.com/mdkhanga/kvstore/config"
	pb "github.com/mdkhanga/kvstore/kvmessages"
	"github.com/mdkhanga/kvstore/logger"
)

var (
	ClusterService = New()
	Log            = logger.WithComponent("cluster").Log
)

type cluster struct {
	mu         sync.Mutex
	clusterMap map[string]*Peer
}

type IClusterService interface {
	AddToCluster(m *Peer) error
	RemoveFromCluster(Hostname string, port int32) error
	ListCluster() ([]*Peer, error)
	Exists(Hostnanme string, port int32) (bool, error)
	ClusterInfoGossip()
}

func (c *cluster) AddToCluster(m *Peer) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	key := fmt.Sprintf("%s:%d", *m.Host, *m.Port)
	if _, exists := c.clusterMap[key]; exists {
		return fmt.Errorf("member %s already exists in the cluster", key)
	}

	c.clusterMap[key] = m
	return nil
}

func (c *cluster) RemoveFromCluster(Hostname string, port int32) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	key := fmt.Sprintf("%s:%d", Hostname, port)
	if _, exists := c.clusterMap[key]; !exists {
		return fmt.Errorf("member %s does not exist in the cluster", key)
	}

	delete(c.clusterMap, key)
	return nil
}

func (c *cluster) ListCluster() ([]*Peer, error) {

	c.mu.Lock()
	defer c.mu.Unlock()

	members := make([]*Peer, 0, len(c.clusterMap))
	for _, member := range c.clusterMap {
		members = append(members, member)
	}
	return members, nil
}

func (c *cluster) Exists(hostname string, port int32) (bool, error) {
	key := fmt.Sprintf("%s:%d", hostname, port)
	if _, exists := c.clusterMap[key]; !exists {
		return false, nil
	}
	return true, nil
}

func New() IClusterService {

	return &cluster{
		mu:         sync.Mutex{},
		clusterMap: make(map[string]*Peer),
	}
}

func (c *cluster) ClusterInfoGossip() {

	cfg := config.GetConfig()

	for {

		var items []string
		members := make([]*pb.Member, len(c.clusterMap))
		for _, pr := range c.clusterMap {
			items = append(items, fmt.Sprintf("%s:%d", *pr.Host, *pr.Port))

			members = append(members, &pb.Member{Hostname: *pr.Host, Port: *pr.Port})

			if *pr.Host == cfg.Hostname && *pr.Port == cfg.Port {
				continue
			}

		}

		cls := pb.Cluster{Members: members}

		clsReq := pb.ClusterInfoRequest{Cluster: &cls}

		clsServerMsg := pb.ServerMessage{
			Type:    pb.MessageType_CLUSTER_INFO_REQUEST,
			Content: &pb.ServerMessage_ClusterInfoRequest{ClusterInfoRequest: &clsReq},
		}

		for _, pr := range c.clusterMap {

			if *pr.Host == cfg.Hostname && *pr.Port == cfg.Port {
				continue
			}

			Log.Info().Str("Sending cluster info msg to", *pr.Host).Int32("and", *pr.Port).Send()
			pr.outMessages.Enqueue(&clsServerMsg)

		}

		result := strings.Join(items, ", ")
		Log.Info().Str("Cluster members", result).Send()
		time.Sleep(5 * time.Second) // Wait before checking again
		continue
	}

}
