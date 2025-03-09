package storage

import (
	"sync"

	m "github.com/mdkhanga/dynago/models"
)

type MemoryStore struct {
	kvMap map[string]string
	mu    sync.Mutex
}

var (
	Store = MemoryStore{kvMap: make(map[string]string), mu: sync.Mutex{}}
)

func (s *MemoryStore) Set(kv *m.KeyValue) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kvMap[kv.Key] = kv.Value
}

func (s *MemoryStore) Get(key string) *m.KeyValue {
	s.mu.Lock()
	defer s.mu.Unlock()
	value := s.kvMap[key]
	return &m.KeyValue{Key: key, Value: value}
}
