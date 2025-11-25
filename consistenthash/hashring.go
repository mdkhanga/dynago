package consistenthash

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"

	"github.com/mdkhanga/dynago/logger"
)

var (
	Log = logger.WithComponent("consistenthash").Log
)

const (
	// Number of virtual nodes per physical node
	DefaultVirtualNodes = 150
)

// HashRing represents the consistent hash ring
type HashRing struct {
	mu           sync.RWMutex
	ring         []uint32           // Sorted array of hash values
	nodeMap      map[uint32]string  // Maps hash value to node ID
	nodes        map[string]bool    // Set of all physical nodes
	virtualNodes int                // Number of virtual nodes per physical node
}

// NewHashRing creates a new hash ring with specified number of virtual nodes
func NewHashRing(virtualNodes int) *HashRing {
	if virtualNodes <= 0 {
		virtualNodes = DefaultVirtualNodes
	}

	return &HashRing{
		ring:         make([]uint32, 0),
		nodeMap:      make(map[uint32]string),
		nodes:        make(map[string]bool),
		virtualNodes: virtualNodes,
	}
}

// hash generates a 32-bit hash value from a string
func (h *HashRing) hash(key string) uint32 {
	hash := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint32(hash[:4])
}

// AddNode adds a physical node to the hash ring
func (h *HashRing) AddNode(nodeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.nodes[nodeID] {
		Log.Info().Str("node", nodeID).Msg("Node already exists in hash ring")
		return
	}

	h.nodes[nodeID] = true

	// Add virtual nodes
	for i := 0; i < h.virtualNodes; i++ {
		virtualKey := fmt.Sprintf("%s#%d", nodeID, i)
		hashValue := h.hash(virtualKey)

		h.ring = append(h.ring, hashValue)
		h.nodeMap[hashValue] = nodeID
	}

	// Sort the ring
	sort.Slice(h.ring, func(i, j int) bool {
		return h.ring[i] < h.ring[j]
	})

	Log.Info().
		Str("node", nodeID).
		Int("virtual_nodes", h.virtualNodes).
		Int("total_ring_size", len(h.ring)).
		Msg("Added node to hash ring")
}

// RemoveNode removes a physical node from the hash ring
func (h *HashRing) RemoveNode(nodeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.nodes[nodeID] {
		Log.Warn().Str("node", nodeID).Msg("Node does not exist in hash ring")
		return
	}

	delete(h.nodes, nodeID)

	// Remove all virtual nodes for this physical node
	newRing := make([]uint32, 0, len(h.ring))
	for _, hashValue := range h.ring {
		if h.nodeMap[hashValue] != nodeID {
			newRing = append(newRing, hashValue)
		} else {
			delete(h.nodeMap, hashValue)
		}
	}

	h.ring = newRing

	Log.Info().
		Str("node", nodeID).
		Int("total_ring_size", len(h.ring)).
		Msg("Removed node from hash ring")
}

// GetNode returns the node responsible for storing the given key
func (h *HashRing) GetNode(key string) (string, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ring) == 0 {
		return "", false
	}

	hashValue := h.hash(key)

	// Binary search to find the first node with hash >= key hash
	idx := sort.Search(len(h.ring), func(i int) bool {
		return h.ring[i] >= hashValue
	})

	// Wrap around to the beginning if necessary
	if idx >= len(h.ring) {
		idx = 0
	}

	nodeID := h.nodeMap[h.ring[idx]]

	Log.Debug().
		Str("key", key).
		Str("node", nodeID).
		Uint32("key_hash", hashValue).
		Uint32("ring_position", h.ring[idx]).
		Msg("Found node for key")

	return nodeID, true
}

// GetNodes returns all physical nodes in the ring
func (h *HashRing) GetNodes() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	nodes := make([]string, 0, len(h.nodes))
	for nodeID := range h.nodes {
		nodes = append(nodes, nodeID)
	}
	return nodes
}

// Size returns the number of physical nodes in the ring
func (h *HashRing) Size() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.nodes)
}

// GetDistribution returns a map showing how many virtual nodes each physical node has
// Useful for debugging and ensuring uniform distribution
func (h *HashRing) GetDistribution() map[string]int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	distribution := make(map[string]int)
	for _, nodeID := range h.nodeMap {
		distribution[nodeID]++
	}
	return distribution
}
