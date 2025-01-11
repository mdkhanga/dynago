package cluster

import (
	"fmt"
	"strings"
	"sync"

	"time"

	"github.com/mdkhanga/dynago/config"
	pb "github.com/mdkhanga/dynago/kvmessages"
	"github.com/mdkhanga/dynago/logger"
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
	MergePeerLists(received []*pb.Member)
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

	// delete(c.clusterMap, key)
	c.clusterMap[key].Status = 1
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

		i := 0
		for _, pr := range c.clusterMap {

			if *&pr.Status != 0 {
				continue
			}

			items = append(items, fmt.Sprintf("%s:%d", *pr.Host, *pr.Port))

			members[i] = &pb.Member{Hostname: *pr.Host, Port: *pr.Port}
			i++

		}

		cls := pb.Cluster{Members: members}

		clsReq := pb.ClusterInfoRequest{Cluster: &cls}

		clsServerMsg := pb.ServerMessage{
			Type:    pb.MessageType_CLUSTER_INFO_REQUEST,
			Content: &pb.ServerMessage_ClusterInfoRequest{ClusterInfoRequest: &clsReq},
		}

		for _, pr := range c.clusterMap {

			if *pr.Host == cfg.Hostname && *pr.Port == cfg.GrpcPort {
				continue
			}

			Log.Info().Str("Sending cluster info msg to", *pr.Host).Int32("and", *pr.Port).Send()
			pr.OutMessages.Enqueue(&clsServerMsg)

		}

		result := strings.Join(items, ", ")
		Log.Info().Str("Cluster members", result).Send()
		time.Sleep(3 * time.Second) // Wait before checking again
		continue
	}

}

func (c *cluster) MergePeerLists(received []*pb.Member) {

	Log.Info().Msg("Merging peer lists")

	for _, m := range received {

		key := fmt.Sprintf("%s:%d", m.Hostname, m.Port)

		if existingPeer, exists := c.clusterMap[key]; exists {
			// Conflict resolution based on timestamp
			if m.Timestamp > existingPeer.Timestamp {
				c.clusterMap[key] = &Peer{
					Host:      &m.Hostname,
					Port:      &m.Port,
					Timestamp: m.Timestamp,
					Status:    int(m.Status.Number()),
				}
			}
		} else {
			// New peer
			c.clusterMap[key] = &Peer{
				Host:      &m.Hostname,
				Port:      &m.Port,
				Timestamp: m.Timestamp,
				Status:    int(m.Status.Number()),
			}
		}

	}

}
