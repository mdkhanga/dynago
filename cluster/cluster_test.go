package cluster_test

import (
	"time"

	"github.com/onsi/ginkgo/v2"

	"github.com/mdkhanga/dynago/server"
)

var _ = ginkgo.Describe("cluster tests", func() {
	var (
		serverA server.IServer
		serverB server.IServer
		serverC server.IServer
	)

	ginkgo.BeforeEach(func() {
		// Initialize servers with unique addresses
		serverA = server.New("localhost", 8090, 8080, "")
		serverB = server.New("localhost", 8091, 8081, "localhost:8090")
		serverC = server.New("localhost", 8092, 8082, "locahost:8090")

		// Start the servers
		go serverA.Start()
		go serverB.Start()
		go serverC.Start()

		// Give servers time to start
		time.Sleep(1 * time.Second)
	})

	ginkgo.AfterEach(func() {
		// Stop the servers
		serverA.Stop()
		serverB.Stop()
		serverC.Stop()
	})

	ginkgo.It("should synchronize peer lists across all servers", func() {

		/*
			// Introduce servers to each other
			serverA.AddPeer("localhost:8081")
			serverB.AddPeer("localhost:8082")

			// Allow time for gossip protocol to run
			time.Sleep(3 * time.Second)

			// Collect peer lists from all servers
			peersA := serverA.GetPeerList()
			peersB := serverB.GetPeerList()
			peersC := serverC.GetPeerList()

			// Assert that all servers have the same peer list
			gomega.Expect(peersA).To(gomega.ConsistOf(peersB))
			gomega.Expect(peersA).To(gomega.ConsistOf(peersC))
		*/
	})
})
