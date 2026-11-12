//go:build e2e

package e2e

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// startThreeNodes starts A, then B and C seeded from A, and waits for
// membership to converge.
func startThreeNodes() []*Node {
	GinkgoHelper()

	a := StartNode("A", nil)
	b := StartNode("B", a)
	c := StartNode("C", a)
	nodes := []*Node{a, b, c}
	expectMembers(convergeTimeout, nodes, a, b, c)
	return nodes
}

var _ = Describe("Key routing", func() {

	It("spreads keys across all nodes", func() {
		const numKeys = 3000

		By("starting A, then B and C seeded from A")
		nodes := startThreeNodes()

		By(fmt.Sprintf("writing %d keys round-robin across A, B and C", numKeys))
		counts := map[string]int{}
		for i := 0; i < numKeys; i++ {
			coordinator := nodes[i%len(nodes)]
			owner, err := coordinator.Set(fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))
			Expect(err).NotTo(HaveOccurred())
			counts[owner]++
		}
		GinkgoWriter.Printf("keys per owner: %v\n", counts)

		By("checking every node owns roughly a third of the keys")
		Expect(counts).To(HaveLen(len(nodes)))
		even := numKeys / len(nodes)
		for _, n := range nodes {
			Expect(counts[n.Addr()]).To(BeNumerically("~", even, even/2),
				"keys owned by %s", n.Name)
		}
	})

	// Pending until Phase 1: nodes learned via gossip are not added to the
	// hash ring, so different coordinators can pick different owners.
	PIt("routes each key to the same owner from every node", func() {
		const numKeys = 50

		By("starting A, then B and C seeded from A")
		nodes := startThreeNodes()

		By(fmt.Sprintf("writing %d keys round-robin across A, B and C", numKeys))
		for i := 0; i < numKeys; i++ {
			coordinator := nodes[i%len(nodes)]
			_, err := coordinator.Set(fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))
			Expect(err).NotTo(HaveOccurred())
		}

		By("reading every key through every node")
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("key-%d", i)
			owners := map[string]bool{}
			for _, n := range nodes {
				result, err := n.Get(key)
				Expect(err).NotTo(HaveOccurred())
				Expect(result.Value).To(Equal(fmt.Sprintf("value-%d", i)),
					"value of %s read via %s", key, n.Name)
				owners[result.Node] = true
			}
			Expect(owners).To(HaveLen(1), "owners reported for %s", key)
		}
	})
})
