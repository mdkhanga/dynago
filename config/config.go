package config

import (
	"sync"
)

type Config struct {
	Hostname string
	GrpcPort int32
	HttpPort int32
}

var (
	config *Config
	once   sync.Once
)

func Init(hostname string, grpcport int32, httpport int32) {
	once.Do(func() {
		config = &Config{
			Hostname: hostname,
			GrpcPort: grpcport,
			HttpPort: httpport,
		}
	})
}

func GetConfig() *Config {
	if config == nil {
		panic("config not initialized")
	}
	return config
}
