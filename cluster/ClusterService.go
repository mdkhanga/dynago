package cluster

import (
	"fmt"
	// "strings"
	"sync"

	"time"

	"github.com/mdkhanga/dynago/config"
	pb "github.com/mdkhanga/dynago/kvmessages"
	"github.com/mdkhanga/dynago/logger"
	"github.com/mdkhanga/dynago/models"
)

var (
	ClusterService               = New()
	Log                          = logger.WithComponent("cluster").Log
	StopGossip     chan struct{} = make(chan struct{})
	once           sync.Once
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
	MergePeerLists(received []*pb.Member, response bool) []*pb.Member
	Replicate(kv *models.KeyValue)
	Stop()
	Start()
}

func (c *cluster) Start() {

	go c.ClusterInfoGossip()
}

func (c *cluster) Close() {
	once.Do(func() {
		Log.Info().Msg("c close called")
		if StopGossip != nil {
			close(StopGossip)
		}
	})

}

func (c *cluster) Stop() {

	for _, p := range c.clusterMap {
		p.Stop()
	}

	c.Close()

}

func (c *cluster) AddToCluster(m *Peer) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	key := fmt.Sprintf("%s:%d", *m.Host, *m.Port)

	m.Status = 0
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

	c.mu.Lock()
	defer c.mu.Unlock()

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

		select {
		case <-StopGossip:
			return
		case <-time.After(1 * time.Second):

			var items []string

			i := 0

			c.mu.Lock()

			members := make([]*pb.Member, len(c.clusterMap))

			for _, pr := range c.clusterMap {

				now := time.Now().UnixMilli()

				pr.mu.Lock()
				if *pr.Host == cfg.Hostname && *pr.Port == cfg.GrpcPort {
					pr.Timestamp = time.Now().UnixMilli()
				}

				if now-pr.Timestamp > 15000 && pr.Mine == false {
					pr.Status = 1 // Mark as inactive
					pr.Stop()

				} else {
					items = append(items, fmt.Sprintf("%s:%d", *pr.Host, *pr.Port))
				}

				members[i] = &pb.Member{Hostname: *pr.Host, Port: *pr.Port, Timestamp: pr.Timestamp, Status: int32(pr.Status)}

				i++

				pr.mu.Unlock()

			}
			c.mu.Unlock()

			cls := pb.Cluster{Members: members}

			clsReq := pb.ClusterInfoRequest{Cluster: &cls}

			clsServerMsg := pb.ServerMessage{
				Type:    pb.MessageType_CLUSTER_INFO_REQUEST,
				Content: &pb.ServerMessage_ClusterInfoRequest{ClusterInfoRequest: &clsReq},
			}

			c.mu.Lock()
			for _, pr := range c.clusterMap {

				if *pr.Host == cfg.Hostname && *pr.Port == cfg.GrpcPort {
					continue
				}

				if pr.Clientend != true {
					continue
				}

				if pr.Status == 1 {
					continue
				}

				pr.OutMessagesChan <- &clsServerMsg

			}
			c.mu.Unlock()

			// result := strings.Join(items, ", ")
			// Log.Info().Str("Cluster members", result).Send()

		}

	}

}

func (c *cluster) Replicate(kv *models.KeyValue) {

	cfg := config.GetConfig()

	kvmsg := pb.KeyValueMessage{Key: kv.Key, Value: kv.Value}
	servermsg := pb.ServerMessage{
		Type:    pb.MessageType_KEY_VALUE,
		Content: &pb.ServerMessage_KeyValue{KeyValue: &kvmsg},
	}

	c.mu.Lock()

	for key, pr := range c.clusterMap {

		Log.Info().Str("Replicating to ", key).Send()

		if *pr.Host == cfg.Hostname && *pr.Port == cfg.GrpcPort {
			continue
		}

		pr.OutMessages.Enqueue(&servermsg)

	}
	c.mu.Unlock()

}

func (c *cluster) MergePeerLists(received []*pb.Member, response bool) []*pb.Member {

	cfg := config.GetConfig()

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, m := range received {

		key := fmt.Sprintf("%s:%d", m.Hostname, m.Port)

		if existingPeer, exists := c.clusterMap[key]; exists {

			// Conflict resolution based on timestamp
			if m.Timestamp >= existingPeer.Timestamp {

				existingPeer.mu.Lock()
				existingPeer.Timestamp = m.Timestamp
				existingPeer.Status = int(m.Status)
				existingPeer.mu.Unlock()

			}
		} else {

			// New peer
			c.clusterMap[key] = &Peer{
				Host:      &m.Hostname,
				Port:      &m.Port,
				Timestamp: m.Timestamp,
				Status:    int(m.Status),
			}

		}

	}

	// send back updates to the sender
	var responseMembers []*pb.Member

	if response {
		// Create a map for quick lookup of received members
		receivedMap := make(map[string]*pb.Member)
		for _, member := range received {
			key := fmt.Sprintf("%s:%d", member.Hostname, member.Port)
			receivedMap[key] = member
		}

		// Check for additional members or members with a different status and more recent timestamp
		for key, peer := range c.clusterMap {
			receivedMember, exists := receivedMap[key]

			if exists && (*peer.Host == cfg.Hostname && *peer.Port == cfg.GrpcPort) {
				continue
			}

			if !exists || peer.Timestamp >= receivedMember.Timestamp {

				responseMembers = append(responseMembers, &pb.Member{
					Hostname:  *peer.Host,
					Port:      *peer.Port,
					Timestamp: peer.Timestamp,
					Status:    int32(peer.Status),
				})
			}
		}
	}

	return responseMembers

}
