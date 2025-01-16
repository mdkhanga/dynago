package server

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/mdkhanga/dynago/cluster"
	"github.com/mdkhanga/dynago/config"
	client "github.com/mdkhanga/dynago/grpcclient"
	"github.com/mdkhanga/dynago/grpcserver"
	"github.com/mdkhanga/dynago/logger"
	m "github.com/mdkhanga/dynago/models"
)

var (
	Log = logger.WithComponent("server").Log
)

type server struct {
	Host       string
	HttpPort   int32
	GrpcPort   int32
	Seed       string
	kvMap      map[string]string
	httpServer *http.Server
}

type IServer interface {
	Start()
	Stop()
	GetPeerList() []string
}

func New(host string, grpcport int32, httpPort int32, seed string) IServer {

	return &server{Host: host, HttpPort: httpPort, GrpcPort: grpcport, Seed: seed}
}

func (s *server) Start() {

	Log.Info().Msg("Starting Dynago server ")

	config.Init(s.Host, s.GrpcPort, s.HttpPort)

	cluster.ClusterService.AddToCluster(&cluster.Peer{Host: &s.Host, Port: &s.GrpcPort, Timestamp: time.Now().UnixMilli(), Status: 0, Mine: true})
	go cluster.ClusterService.ClusterInfoGossip()
	// go cluster.ClusterService.MonitorPeers()

	go grpcserver.StartGrpcServer(&s.Host, &s.GrpcPort)

	if s.Seed != "" {
		go client.CallGrpcServer(&s.Host, &s.GrpcPort, &s.Seed)
	}

	router := gin.Default()
	router.GET("/kvstore", getInfo)
	router.GET("/kvstore/:key", s.getValue)
	router.POST("/kvstore", s.setValue)

	rbind := fmt.Sprintf("%s:%d", s.Host, s.HttpPort)
	router.Run(rbind)

}

func (s *server) Stop() {

}

func (s *server) startGinServer(rbind string) {
	router := gin.Default()

	// Define your routes here
	router.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "pong"})
	})

	// Initialize the HTTP server
	s.httpServer = &http.Server{
		Addr:    rbind,
		Handler: router,
	}

	Log.Info().Str("Starting server at %s\n", rbind).Send()
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		Log.Error().AnErr("Server failed to start: %v\n", err)

	}

}

func (s *server) StopGinServer() {
	if s.httpServer != nil {
		Log.Info().Msg("Stopping server immediately...")

		// Forcefully close the server
		if err := s.httpServer.Close(); err != nil {
			Log.Fatal().AnErr("Server forced to shutdown with error: %v\n", err)
		}
		Log.Info().Msg("Server stopped immediately.")
	} else {
		Log.Info().Msg("Server is not running.")
	}
}

func getInfo(c *gin.Context) {
	c.JSON(http.StatusOK, "Welcome to keystore")
}

func (s *server) getValue(c *gin.Context) {
	key := c.Param("key")
	value := s.kvMap[key]
	jsonString := fmt.Sprintf("{\"%s\":\"%s\"}", key, value)
	c.JSON(http.StatusOK, jsonString)
}

func (s *server) setValue(c *gin.Context) {
	var input m.KeyValue
	c.BindJSON(&input)
	s.kvMap[input.Key] = input.Value
	c.JSON(http.StatusOK, "Welcome to keystore")
}

func (s *server) GetPeerList() []string {

	peers, _ := cluster.ClusterService.ListCluster()

	peerhostports := make([]string, len(peers))

	for i, p := range peers {
		ph := fmt.Sprintf("%s:%d", *p.Host, *p.Port)
		peerhostports[i] = ph

	}

	return peerhostports

}
