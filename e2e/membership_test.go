//go:build e2e

package e2e

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	// Nodes gossip every second; a fresh cluster should converge quickly.
	convergeTimeout = 30 * time.Second
	// A stopped node is marked inactive after 15s without updates, plus gossip delay.
	failureTimeout = 40 * time.Second
	pollInterval   = 500 * time.Millisecond
)

// expectMembers waits until every node in nodes reports exactly the expected members.
func expectMembers(timeout time.Duration, nodes []*Node, expected ...*Node) {
	GinkgoHelper()

	addrs := make([]string, len(expected))
	for i, n := range expected {
		addrs[i] = n.Addr()
	}

	for _, n := range nodes {
		Eventually(n.Members).WithTimeout(timeout).WithPolling(pollInterval).
			Should(ConsistOf(addrs), "members reported by %s", n.Name)
	}
}

var _ = Describe("Cluster membership", func() {

	It("tracks nodes joining, leaving and rejoining", func() {
		By("starting A, then B and C seeded from A")
		a := StartNode("A", nil)
		b := StartNode("B", a)
		c := StartNode("C", a)

		By("waiting for all nodes to see A, B and C")
		expectMembers(convergeTimeout, []*Node{a, b, c}, a, b, c)

		By("stopping B")
		b.Stop()

		By("waiting for A and C to drop B")
		expectMembers(failureTimeout, []*Node{a, c}, a, c)

		By("restarting B on the same ports")
		b.Start()

		By("waiting for all nodes to see A, B and C again")
		expectMembers(convergeTimeout, []*Node{a, b, c}, a, b, c)
	})
})
