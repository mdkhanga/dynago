package utils

import (
	"sync"
)

// SafeMap is a generic thread-safe map
type SafeMap[K comparable, V any] struct {
	mu  sync.Mutex
	Map map[K]V
}

// Set adds or updates a key-value pair in the map
func (sm *SafeMap[K, V]) Set(key K, value V) {
	sm.mu.Lock()         // Lock for safe access
	defer sm.mu.Unlock() // Unlock when done
	sm.Map[key] = value
}

// Get retrieves a value by its key, and a boolean indicating if the key exists
func (sm *SafeMap[K, V]) Get(key K) (V, bool) {
	sm.mu.Lock()         // Lock for safe access
	defer sm.mu.Unlock() // Unlock when done
	value, exists := sm.Map[key]
	return value, exists
}

// GetAllPairs safely returns a slice of K,V pairs from the SafeMap
func (sm *SafeMap[K, V]) GetAllPairs() []struct {
	Key   K
	Value V
} {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var pairs []struct {
		Key   K
		Value V
	}
	for key, value := range sm.Map {
		pairs = append(pairs, struct {
			Key   K
			Value V
		}{Key: key, Value: value})
	}

	return pairs
}
