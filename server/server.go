package server

import (
	"fmt"
	"net/http"

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
	Host     string
	HttpPort int32
	GrpcPort int32
	Seed     string
	kvMap    map[string]string
}

type IServer interface {
	Start()
	Stop()
}

func New(host string, grpcport int32, httpPort int32, seed string) IServer {

	return &server{Host: host, HttpPort: httpPort, GrpcPort: grpcport, Seed: seed}
}

func (s *server) Start() {

	Log.Info().Msg("Starting Dynago server ")

	config.Init(s.Host, s.GrpcPort, s.HttpPort)

	cluster.ClusterService.AddToCluster(&cluster.Peer{Host: &s.Host, Port: &s.GrpcPort})
	go cluster.ClusterService.ClusterInfoGossip()

	go grpcserver.StartGrpcServer(&s.Host, &s.GrpcPort)

	if s.Seed != "" {
		go client.CallGrpcServerv2(&s.Host, &s.GrpcPort, &s.Seed)
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
