package config

import (
	"sync"
)

type Config struct {
	Hostname string
	Port     int32
}

var (
	config *Config
	once   sync.Once
)

// Initialize the configuration (singleton)
func Init(hostname string, port int32) {
	once.Do(func() {
		config = &Config{
			Hostname: hostname,
			Port:     port,
		}
	})
}

// GetConfig returns the global configuration instance
func GetConfig() *Config {
	if config == nil {
		panic("config not initialized")
	}
	return config
}
