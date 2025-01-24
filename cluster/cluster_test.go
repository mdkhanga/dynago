package cluster_test

import (
	"time"

	"github.com/onsi/ginkgo/v2"

	"fmt"

	"github.com/mdkhanga/dynago/logger"
	"github.com/mdkhanga/dynago/server"
	"github.com/onsi/gomega"
)

/* Tests for cluster setup
 */

var _ = ginkgo.Describe("cluster tests", func() {
	var ()

	ginkgo.BeforeEach(func() {

	})

	ginkgo.AfterEach(func() {

	})

	ginkgo.It("basic serv sync", func() {

		logger.Globallogger.Info("Test")

		// Initialize servers with unique addresses
		serverA := server.New("localhost", 8090, 8080, "")
		serverB := server.New("localhost", 8091, 8081, "localhost:8090")
		serverC := server.New("localhost", 8092, 8082, "locahost:8090")

		// Start the servers
		go serverA.Start()
		time.Sleep(2 * time.Second)
		go serverB.Start()
		time.Sleep(2 * time.Second)
		go serverC.Start()
		time.Sleep(3 * time.Second)

		peersA := serverA.GetPeerList()
		peersB := serverB.GetPeerList()
		peersC := serverC.GetPeerList()

		logger.Globallogger.Log.Info().Any("A list", peersA).Send()
		logger.Globallogger.Log.Info().Any("B list", peersB).Send()
		logger.Globallogger.Log.Info().Any("C list", peersC).Send()

		// Assert that all servers have the same peer list
		gomega.Expect(peersA).To(gomega.ConsistOf(peersB))
		gomega.Expect(peersA).To(gomega.ConsistOf(peersC))

		serverC.Stop()
		time.Sleep(2 * time.Second)
		serverB.Stop()
		time.Sleep(2 * time.Second)
		serverC.Stop()
		time.Sleep(2 * time.Second)

	})

	ginkgo.It("drop server", func() {

		fmt.Println("Hello mjjjjjjj")
		logger.Globallogger.Info("Test")

		// Initialize servers with unique addresses
		serverA := server.New("localhost", 9000, 8080, "")
		serverB := server.New("localhost", 9001, 8081, "localhost:9000")
		serverC := server.New("localhost", 9002, 8082, "locahost:9000")

		// Start the servers
		go serverA.Start()
		time.Sleep(2 * time.Second)
		go serverB.Start()
		time.Sleep(2 * time.Second)
		go serverC.Start()
		time.Sleep(3 * time.Second)

		peersA := serverA.GetPeerList()
		peersB := serverB.GetPeerList()
		peersC := serverC.GetPeerList()

		logger.Globallogger.Log.Info().Any("C list", peersC).Send()

		// Assert that all servers have the same peer list
		gomega.Expect(peersA).To(gomega.ConsistOf(peersB))
		gomega.Expect(peersA).To(gomega.ConsistOf(peersC))

		serverC.Stop()
		time.Sleep(20 * time.Second)
		fmt.Println("Server C has stopped")

		peersA = serverA.GetPeerList()
		peersB = serverB.GetPeerList()

		fmt.Println("PeersA", peersA)
		fmt.Println("PeersB", peersB)
		time.Sleep(3 * time.Second)
		ginkgo.GinkgoWriter.Printf("PeersA", peersA)
		ginkgo.GinkgoWriter.Printf("PeersB", peersB)
		logger.Globallogger.Log.Info().Any("A list", peersA).Send()
		logger.Globallogger.Log.Info().Any("B list", peersB).Send()
		gomega.Expect(peersA).To(gomega.ConsistOf(peersB))
		gomega.Expect(len(peersA)).To(gomega.Equal(2))
		gomega.Expect(len(peersB)).To(gomega.Equal(2))

		serverB.Stop()
		time.Sleep(2 * time.Second)
		serverC.Stop()
		time.Sleep(2 * time.Second)

	})
})
