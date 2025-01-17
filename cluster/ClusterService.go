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
	ClusterService               = New()
	Log                          = logger.WithComponent("cluster").Log
	StopGossip     chan struct{} = make(chan struct{})
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
	MonitorPeers()
	Stop()
	Start()
}

func (c *cluster) Start() {

	go c.ClusterInfoGossip()
}

func (c *cluster) Stop() {

	close(StopGossip)

	for _, p := range c.clusterMap {
		p.Stop()
	}

}

func (c *cluster) AddToCluster(m *Peer) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	key := fmt.Sprintf("%s:%d", *m.Host, *m.Port)
	if _, exists := c.clusterMap[key]; exists {
		return fmt.Errorf("member %s already exists in the cluster", key)
	}

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

		select {
		case <-StopGossip:
			// Signal received to stop the loop
			Log.Info().Msg("Stopping clusterInfoGossip.")
			return
		default:
			// Do your stuff here
			Log.Info().Msg("Starting Gossip ..........")
			time.Sleep(1 * time.Second) // Simulating work

			var items []string
			members := make([]*pb.Member, len(c.clusterMap))

			i := 0
			for key, pr := range c.clusterMap {

				now := time.Now().UnixMilli()

				if *pr.Host == cfg.Hostname && *pr.Port == cfg.GrpcPort {
					pr.Timestamp = time.Now().UnixMilli()
				}

				if now-pr.Timestamp > 10000 && pr.Mine == false {
					pr.Status = 1 // Mark as inactive
					Log.Info().Str("Peer marked as inactive", key).Int64("now", now).Int64("peer timestamp", pr.Timestamp).Send()
					continue
				}

				items = append(items, fmt.Sprintf("%s:%d", *pr.Host, *pr.Port))

				members[i] = &pb.Member{Hostname: *pr.Host, Port: *pr.Port, Timestamp: pr.Timestamp, Status: int32(pr.Status)}

				Log.Debug().Any("Peer", pr).Send()
				Log.Debug().Any("Member", members[i]).Send()

				i++

			}

			// Log.Debug().Any("Members to send", members).Send()

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

				//Log.Info().Str("Sending cluster info msg to", *pr.Host).Int32("and", *pr.Port).Send()
				pr.OutMessages.Enqueue(&clsServerMsg)

			}

			result := strings.Join(items, ", ")
			Log.Info().Str("Cluster members", result).Send()
			time.Sleep(3 * time.Second) // Wait before checking again
			continue
		}

	}

}

func (c *cluster) MergePeerLists(received []*pb.Member) {

	Log.Info().Any("Merging peer lists", received).Send()

	for _, m := range received {

		key := fmt.Sprintf("%s:%d", m.Hostname, m.Port)

		if existingPeer, exists := c.clusterMap[key]; exists {
			// Conflict resolution based on timestamp
			if m.Timestamp > existingPeer.Timestamp {
				c.clusterMap[key] = &Peer{
					Host:      &m.Hostname,
					Port:      &m.Port,
					Timestamp: m.Timestamp,
					Status:    int(m.Status),
				}
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

}

func (c *cluster) MonitorPeers() {
	for {
		// Lock the clusterMap for safe access
		Log.Info().Msg("In the monitor peer")
		c.mu.Lock()
		now := time.Now().UnixMilli()

		for key, peer := range c.clusterMap {
			// Check if the peer's timestamp is older than 5 seconds
			if now-peer.Timestamp > 5000 && peer.Mine == false {
				peer.Status = 1 // Mark as inactive
				// Optionally log or take further action
				Log.Info().Str("Peer marked as inactive", key).Send()
			}
		}

		// Unlock after processing
		c.mu.Unlock()

		// Sleep for a periodic interval (e.g., 1 second)
		time.Sleep(2 * time.Second)
	}
}
