package cluster_test

import (
	"time"

	"github.com/onsi/ginkgo/v2"

	"github.com/mdkhanga/dynago/logger"
	"github.com/mdkhanga/dynago/server"
	"github.com/onsi/gomega"
)

/* Tests for cluster setup
 */

var _ = ginkgo.Describe("cluster add tests", func() {
	var (
	/* serverA server.IServer
	serverB server.IServer
	serverC server.IServer */
	)

	ginkgo.BeforeEach(func() {
		// Initialize servers with unique addresses
		/* serverA = server.New("localhost", 8090, 8080, "")
		serverB = server.New("localhost", 8091, 8081, "localhost:8090")
		serverC = server.New("localhost", 8092, 8082, "locahost:8090")

		// Start the servers
		go serverA.Start()
		time.Sleep(2 * time.Second)
		go serverB.Start()
		time.Sleep(2 * time.Second)
		go serverC.Start()
		time.Sleep(3 * time.Second) */

	})

	ginkgo.AfterEach(func() {
		// Stop the servers
		/* serverA.Stop()
		serverB.Stop()
		serverC.Stop() */
	})

	ginkgo.It("test adding a server", func() {

		logger.Globallogger.Info("Test")

		// Initialize servers with unique addresses
		serverA := server.New("localhost", 8095, 8080, "")
		serverB := server.New("localhost", 8096, 8081, "localhost:8095")

		// Start the servers
		go serverA.Start()
		time.Sleep(2 * time.Second)
		go serverB.Start()
		time.Sleep(2 * time.Second)

		peersA := serverA.GetPeerList()
		peersB := serverB.GetPeerList()

		// Assert that all servers have the same peer list
		gomega.Expect(peersA).To(gomega.ConsistOf(peersB))

		serverC := server.New("localhost", 8097, 8082, "locahost:8095")
		go serverC.Start()
		time.Sleep(4 * time.Second)

		peersA = serverA.GetPeerList()
		peersB = serverB.GetPeerList()
		peersC := serverC.GetPeerList()

		gomega.Expect(peersA).To(gomega.ConsistOf(peersC))
		gomega.Expect(peersB).To(gomega.ConsistOf(peersC))

		logger.Globallogger.Log.Info().Any("A list", peersA).Send()
		logger.Globallogger.Log.Info().Any("B list", peersB).Send()
		logger.Globallogger.Log.Info().Any("C list", peersC).Send()

		serverC.Stop()
		time.Sleep(2 * time.Second)
		serverB.Stop()
		time.Sleep(2 * time.Second)
		serverC.Stop()
		time.Sleep(2 * time.Second)

	})
})
