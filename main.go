package main

import (
	"flag"
	"fmt"
	"net/http"

	"github.com/mdkhanga/dynago/grpcserver"
	m "github.com/mdkhanga/dynago/models"

	client "github.com/mdkhanga/dynago/grpcclient"

	"github.com/gin-gonic/gin"
	"github.com/mdkhanga/dynago/cluster"
	"github.com/mdkhanga/dynago/config"
	"github.com/mdkhanga/dynago/logger"
	"github.com/mdkhanga/dynago/utils"
)

var kvMap map[string]string

var (
// port = flag.Int("port", 50051, "The server port")
)

func main() {

	Logger := logger.Globallogger.Log

	Logger.Info().Msg("Welcome to key value store")

	host := flag.String("i", "localhost", "ipv4 address tp bind to")
	portPtr := flag.String("p", "8081", "tcp port to listenon")
	seed := flag.String("seed", "", "ip of server to connect to")
	httpPort := flag.String("h", "8080", "http port to listenon")

	flag.Parse()

	Logger.Debug().Str("Going to bind to address: ", *host)
	Logger.Info().Str("Going to listen on port ", *portPtr)
	Logger.Debug().Str("Seed to connect to ", *seed).Send()
	Logger.Info().Str("Going to listen on http port ", *httpPort).Send()

	kvMap = make(map[string]string)
	kvMap["hello"] = "world"

	portInt32, _ := utils.StringToInt32(*portPtr)

	config.Init(*host, portInt32)

	// cluster.ClusterService.AddToCluster(&m.ClusterMember{Host: *host, Port: portInt32})
	cluster.ClusterService.AddToCluster(&cluster.Peer{Host: host, Port: &portInt32})
	go cluster.ClusterService.ClusterInfoGossip()

	router := gin.Default()
	router.GET("/kvstore", getInfo)
	router.GET("/kvstore/:key", getValue)
	router.POST("/kvstore", setValue)

	go grpcserver.StartGrpcServer(host, portPtr)

	if *seed != "" {
		go client.CallGrpcServerv2(host, portPtr, seed)
	}

	router.Run(":" + *httpPort)

}

func getInfo(c *gin.Context) {
	c.JSON(http.StatusOK, "Welcome to keystore")
}

func getValue(c *gin.Context) {
	key := c.Param("key")
	value := kvMap[key]
	jsonString := fmt.Sprintf("{\"%s\":\"%s\"}", key, value)
	c.JSON(http.StatusOK, jsonString)
}

func setValue(c *gin.Context) {
	var input m.KeyValue
	c.BindJSON(&input)
	kvMap[input.Key] = input.Value
	c.JSON(http.StatusOK, "Welcome to keystore")
}
