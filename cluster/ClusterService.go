package cluster

import (
	"fmt"
	// "strings"
	"sync"

	"time"

	"github.com/mdkhanga/dynago/config"
	"github.com/mdkhanga/dynago/consistenthash"
	pb "github.com/mdkhanga/dynago/kvmessages"
	"github.com/mdkhanga/dynago/logger"
	"github.com/mdkhanga/dynago/models"
)

var (
	ClusterService               = New()
	HashRing                     = consistenthash.NewHashRing(consistenthash.DefaultVirtualNodes)
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
	GetNodeForKey(key string) (string, bool)
	ForwardGet(nodeID string, key string) (*models.KeyValue, error)
	ForwardSet(nodeID string, key string, value string) error
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

	// Log.Info().Msg("Entering Add to cluster")

	c.mu.Lock()
	defer c.mu.Unlock()

	key := fmt.Sprintf("%s:%d", *m.Host, *m.Port)

	// Check if this is a new node or just an update
	_, existsInCluster := c.clusterMap[key]

	m.Status = 0
	c.clusterMap[key] = m

	// Only add node to hash ring if it's new
	if !existsInCluster {
		HashRing.AddNode(key)
	}

	// Log.Info().Msg("Exiting Add to cluster")
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

	// Remove node from hash ring
	HashRing.RemoveNode(key)

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

		pr.OutMessagesChan <- &servermsg

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

			// Log.Info().Str("Merge found key do not need to call connect", key).Send()

			// Conflict resolution based on timestamp
			if m.Timestamp >= existingPeer.Timestamp {

				existingPeer.mu.Lock()
				existingPeer.Timestamp = m.Timestamp
				existingPeer.Status = int(m.Status)
				existingPeer.mu.Unlock()

			}
		} else {

			// New peer
			p := NewPeerWithoutStream(&m.Hostname, &m.Port, m.Timestamp, int(m.Status))

			// Log.Info().Str("merged did not find key. need to call init and perhaps connect", key).Send()
			c.clusterMap[key] = p
			go p.Init()

			/* c.clusterMap[key] = &Peer{
				Host:      &m.Hostname,
				Port:      &m.Port,
				Timestamp: m.Timestamp,
				Status:    int(m.Status),
			} */

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

// GetNodeForKey returns the node ID (host:port) responsible for storing the given key
func (c *cluster) GetNodeForKey(key string) (string, bool) {
	return HashRing.GetNode(key)
}

// ForwardGet forwards a GET request to the specified node and waits for response
func (c *cluster) ForwardGet(nodeID string, key string) (*models.KeyValue, error) {
	c.mu.Lock()

	// Find a connected peer for this node
	var peer *Peer
	for _, pr := range c.clusterMap {
		peerID := fmt.Sprintf("%s:%d", *pr.Host, *pr.Port)
		if peerID == nodeID && pr.Status == 0 {
			// Prefer client-side peers (Clientend=false) as they initiated the connection
			if peer == nil || pr.Clientend == false {
				peer = pr
			}
		}
	}
	c.mu.Unlock()

	if peer == nil {
		return nil, fmt.Errorf("no active connection to node %s", nodeID)
	}

	Log.Info().
		Str("node", nodeID).
		Str("key", key).
		Bool("clientend", peer.Clientend).
		Msg("Forwarding GET request")

	// Send GET request
	request := &pb.ServerMessage{
		Type: pb.MessageType_FORWARD_GET_REQUEST,
		Content: &pb.ServerMessage_ForwardGetRequest{
			ForwardGetRequest: &pb.ForwardGetRequest{
				Key: key,
			},
		},
	}

	peer.OutMessagesChan <- request

	// Wait for response with timeout
	select {
	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("timeout waiting for GET response from %s", nodeID)
	case resp := <-peer.getResponseChan:
		if !resp.Found {
			return &models.KeyValue{Key: key, Value: ""}, nil
		}
		return &models.KeyValue{Key: resp.Key, Value: resp.Value}, nil
	}
}

// ForwardSet forwards a SET request to the specified node and waits for response
func (c *cluster) ForwardSet(nodeID string, key string, value string) error {
	c.mu.Lock()

	// Find a connected peer for this node
	var peer *Peer
	for _, pr := range c.clusterMap {
		peerID := fmt.Sprintf("%s:%d", *pr.Host, *pr.Port)
		if peerID == nodeID && pr.Status == 0 {
			// Prefer client-side peers (Clientend=false) as they initiated the connection
			if peer == nil || pr.Clientend == false {
				peer = pr
			}
		}
	}
	c.mu.Unlock()

	if peer == nil {
		return fmt.Errorf("no active connection to node %s", nodeID)
	}

	Log.Info().
		Str("node", nodeID).
		Str("key", key).
		Str("value", value).
		Bool("clientend", peer.Clientend).
		Msg("Forwarding SET request")

	// Send SET request
	request := &pb.ServerMessage{
		Type: pb.MessageType_FORWARD_SET_REQUEST,
		Content: &pb.ServerMessage_ForwardSetRequest{
			ForwardSetRequest: &pb.ForwardSetRequest{
				Key:   key,
				Value: value,
			},
		},
	}

	peer.OutMessagesChan <- request

	// Wait for response with timeout
	select {
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout waiting for SET response from %s", nodeID)
	case resp := <-peer.setResponseChan:
		if !resp.Success {
			return fmt.Errorf("SET failed: %s", resp.Error)
		}
		return nil
	}
}
