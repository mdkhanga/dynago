package server

import (
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/mdkhanga/dynago/cluster"
	"github.com/mdkhanga/dynago/config"
	client "github.com/mdkhanga/dynago/grpcclient"
	"github.com/mdkhanga/dynago/grpcserver"
	pb "github.com/mdkhanga/dynago/kvmessages"
	"github.com/mdkhanga/dynago/logger"
	m "github.com/mdkhanga/dynago/models"
	"google.golang.org/grpc"
)

var (
	Log = logger.WithComponent("server").Log
)

type server struct {
	Host         string
	HttpPort     int32
	GrpcPort     int32
	Seed         string
	kvMap        map[string]string
	httpServer   *http.Server
	grpcServer   *grpc.Server
	grpcListener net.Listener
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
	// go cluster.ClusterService.ClusterInfoGossip()
	cluster.ClusterService.Start()

	go s.startGrpcServer(&s.Host, &s.GrpcPort)

	if s.Seed != "" {
		go client.CallGrpcServer(&s.Host, &s.GrpcPort, &s.Seed)
	}

	rbind := fmt.Sprintf("%s:%d", s.Host, s.HttpPort)
	s.startGinServer(rbind)

}

func (s *server) Stop() {

	// close(cluster.StopGossip)
	Log.Info().Msg("shutting down cluster")
	client.Close()
	cluster.ClusterService.Stop()
	time.Sleep(5 * time.Second)
	s.stopGinServer()
	s.stopGrpcServer()

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

	Log.Info().Msg("Exiting server")

}

func (s *server) stopGinServer() {
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

func (s *server) startGrpcServer(hostPtr *string, portPtr *int32) {

	var err error
	s.grpcListener, err = net.Listen("tcp", fmt.Sprintf("%s:%d", *hostPtr, *portPtr))
	// lis, err := net.Listen("tcp", fmt.Sprintf("192.168.1.15:%s", *portPtr))
	if err != nil {
		Log.Error().AnErr("failed to listen:", err).Send()
	}

	s.grpcServer = grpc.NewServer()
	pb.RegisterKVSeviceServer(s.grpcServer, &grpcserver.Server{})
	Log.Info().Any("GRPC server listening at ", s.grpcListener.Addr().String()).Send()
	if err := s.grpcServer.Serve(s.grpcListener); err != nil {
		Log.Error().AnErr("failed to serve: ", err).Send()
	}

}

func (s *server) stopGrpcServer() {
	if s.grpcServer != nil {
		Log.Info().Msg("Stopping gRPC server...")
		// s.grpcServer.GracefulStop() // Gracefully stop the server
		s.grpcServer.Stop()
		s.grpcListener.Close()
		Log.Info().Msg("gRPC server stopped.")
	} else {
		Log.Warn().Msg("gRPC server is not running.")
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

		if p.Status == 1 {
			continue
		}

		ph := fmt.Sprintf("%s:%d", *p.Host, *p.Port)
		peerhostports[i] = ph

	}

	return peerhostports

}
