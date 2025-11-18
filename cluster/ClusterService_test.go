package cluster_test

import (
	"time"

	"github.com/mdkhanga/dynago/cluster"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ClusterService", func() {
	var (
		clusterService cluster.IClusterService
		host1          string
		host2          string
		host3          string
		port1          int32
		port2          int32
		port3          int32
	)

	BeforeEach(func() {
		clusterService = cluster.New()
		host1 = "192.168.1.10"
		host2 = "192.168.1.11"
		host3 = "192.168.1.12"
		port1 = int32(8081)
		port2 = int32(8082)
		port3 = int32(8083)
	})

	Describe("AddToCluster", func() {
		Context("when adding a new peer", func() {
			It("should successfully add the peer to the cluster", func() {
				peer := &cluster.Peer{
					Host:      &host1,
					Port:      &port1,
					Timestamp: time.Now().UnixMilli(),
					Status:    0,
					Mine:      true,
					Clientend: false,
				}

				err := clusterService.AddToCluster(peer)
				Expect(err).ToNot(HaveOccurred())

				exists, err := clusterService.Exists(host1, port1)
				Expect(err).ToNot(HaveOccurred())
				Expect(exists).To(BeTrue())
			})

			It("should set peer status to 0 (active)", func() {
				peer := &cluster.Peer{
					Host:      &host1,
					Port:      &port1,
					Timestamp: time.Now().UnixMilli(),
					Status:    1, // Set as inactive
					Mine:      false,
					Clientend: false,
				}

				err := clusterService.AddToCluster(peer)
				Expect(err).ToNot(HaveOccurred())

				// Status should be set to 0 by AddToCluster
				Expect(peer.Status).To(Equal(0))
			})
		})

		Context("when adding multiple peers", func() {
			It("should add all peers successfully", func() {
				peer1 := &cluster.Peer{Host: &host1, Port: &port1, Timestamp: time.Now().UnixMilli()}
				peer2 := &cluster.Peer{Host: &host2, Port: &port2, Timestamp: time.Now().UnixMilli()}
				peer3 := &cluster.Peer{Host: &host3, Port: &port3, Timestamp: time.Now().UnixMilli()}

				clusterService.AddToCluster(peer1)
				clusterService.AddToCluster(peer2)
				clusterService.AddToCluster(peer3)

				peers, err := clusterService.ListCluster()
				Expect(err).ToNot(HaveOccurred())
				Expect(peers).To(HaveLen(3))
			})
		})
	})

	Describe("RemoveFromCluster", func() {
		Context("when removing an existing peer", func() {
			It("should mark the peer as inactive (status=1)", func() {
				peer := &cluster.Peer{
					Host:      &host1,
					Port:      &port1,
					Timestamp: time.Now().UnixMilli(),
					Status:    0,
				}

				clusterService.AddToCluster(peer)

				err := clusterService.RemoveFromCluster(host1, port1)
				Expect(err).ToNot(HaveOccurred())

				// Peer should still exist but be marked inactive
				exists, _ := clusterService.Exists(host1, port1)
				Expect(exists).To(BeTrue())
				Expect(peer.Status).To(Equal(1))
			})
		})

		Context("when removing a non-existent peer", func() {
			It("should return an error", func() {
				err := clusterService.RemoveFromCluster("nonexistent.host", 9999)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("does not exist"))
			})
		})
	})

	Describe("ListCluster", func() {
		Context("when cluster is empty", func() {
			It("should return an empty list", func() {
				peers, err := clusterService.ListCluster()
				Expect(err).ToNot(HaveOccurred())
				Expect(peers).To(BeEmpty())
			})
		})

		Context("when cluster has multiple peers", func() {
			It("should return all peers", func() {
				peer1 := &cluster.Peer{Host: &host1, Port: &port1, Timestamp: time.Now().UnixMilli()}
				peer2 := &cluster.Peer{Host: &host2, Port: &port2, Timestamp: time.Now().UnixMilli()}

				clusterService.AddToCluster(peer1)
				clusterService.AddToCluster(peer2)

				peers, err := clusterService.ListCluster()
				Expect(err).ToNot(HaveOccurred())
				Expect(peers).To(HaveLen(2))
			})

			It("should include both active and inactive peers", func() {
				peer1 := &cluster.Peer{Host: &host1, Port: &port1, Timestamp: time.Now().UnixMilli(), Status: 0}
				peer2 := &cluster.Peer{Host: &host2, Port: &port2, Timestamp: time.Now().UnixMilli(), Status: 0}

				clusterService.AddToCluster(peer1)
				clusterService.AddToCluster(peer2)
				clusterService.RemoveFromCluster(host1, port1) // Mark peer1 as inactive

				peers, err := clusterService.ListCluster()
				Expect(err).ToNot(HaveOccurred())
				Expect(peers).To(HaveLen(2))
			})
		})
	})

	Describe("Exists", func() {
		Context("when checking for an existing peer", func() {
			It("should return true", func() {
				peer := &cluster.Peer{
					Host:      &host1,
					Port:      &port1,
					Timestamp: time.Now().UnixMilli(),
				}

				clusterService.AddToCluster(peer)

				exists, err := clusterService.Exists(host1, port1)
				Expect(err).ToNot(HaveOccurred())
				Expect(exists).To(BeTrue())
			})
		})

		Context("when checking for a non-existent peer", func() {
			It("should return false", func() {
				exists, err := clusterService.Exists("192.168.1.99", 9999)
				Expect(err).ToNot(HaveOccurred())
				Expect(exists).To(BeFalse())
			})
		})

		Context("when peer was removed (marked inactive)", func() {
			It("should still return true since peer exists in map", func() {
				peer := &cluster.Peer{Host: &host1, Port: &port1, Timestamp: time.Now().UnixMilli()}

				clusterService.AddToCluster(peer)
				clusterService.RemoveFromCluster(host1, port1)

				exists, err := clusterService.Exists(host1, port1)
				Expect(err).ToNot(HaveOccurred())
				Expect(exists).To(BeTrue()) // Still exists, just marked inactive
			})
		})
	})
})
