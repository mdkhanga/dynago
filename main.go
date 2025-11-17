package main

import (
	"flag"

	"github.com/mdkhanga/dynago/logger"
	"github.com/mdkhanga/dynago/server"
	"github.com/mdkhanga/dynago/utils"

	"math/rand"
	"time"
)

func main() {

	Logger := logger.Globallogger.Log

	Logger.Info().Msg("Welcome to key value store")

	rand.Seed(time.Now().UnixNano())

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

	httpPortInt32, _ := utils.StringToInt32(*httpPort)
	server := server.New(*host, portInt32, httpPortInt32, *seed)
	server.Start()

}
