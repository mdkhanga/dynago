package server

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/mdkhanga/dynago/logger"
	m "github.com/mdkhanga/dynago/models"
)

var (
	Log = logger.WithComponent("server").Log
)

type server struct {
	Host  string
	Port  int32
	kvMap map[string]string
}

type IServer interface {
	Start()
	Stop()
}

func New(host string, port int32) IServer {

	return &server{Host: host, Port: port}
}

func (s *server) Start() {

	Log.Info().Msg("Starting Dynago server ")

	router := gin.Default()
	router.GET("/kvstore", getInfo)
	router.GET("/kvstore/:key", s.getValue)
	router.POST("/kvstore", s.setValue)

	rbind := fmt.Sprintf("%s:%d", s.Host, s.Port)
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
