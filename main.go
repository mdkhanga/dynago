package main

import (
	"flag"

	"github.com/mdkhanga/dynago/grpcserver"

	client "github.com/mdkhanga/dynago/grpcclient"

	"github.com/mdkhanga/dynago/cluster"
	"github.com/mdkhanga/dynago/config"
	"github.com/mdkhanga/dynago/logger"
	"github.com/mdkhanga/dynago/server"
	"github.com/mdkhanga/dynago/utils"
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

	portInt32, _ := utils.StringToInt32(*portPtr)

	config.Init(*host, portInt32)

	cluster.ClusterService.AddToCluster(&cluster.Peer{Host: host, Port: &portInt32})
	go cluster.ClusterService.ClusterInfoGossip()

	go grpcserver.StartGrpcServer(host, portPtr)

	if *seed != "" {
		go client.CallGrpcServerv2(host, portPtr, seed)
	}

	httpPortInt32, _ := utils.StringToInt32(*httpPort)
	server := server.New(*host, httpPortInt32)
	server.Start()

}
