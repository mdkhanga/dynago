//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

const host = "localhost"

var httpClient = &http.Client{Timeout: 2 * time.Second}

// Node is a dynago server running as a separate process.
type Node struct {
	Name     string
	GrpcPort int
	HttpPort int
	seed     *Node
	session  *gexec.Session
}

// Addr is the node's cluster identity as reported by /members.
func (n *Node) Addr() string {
	return fmt.Sprintf("%s:%d", host, n.GrpcPort)
}

func (n *Node) url(path string) string {
	return fmt.Sprintf("http://%s:%d%s", host, n.HttpPort, path)
}

// StartNode picks free ports and starts a node. If seed is non-nil the node
// joins the cluster through it. The node is stopped when the spec ends.
func StartNode(name string, seed *Node) *Node {
	n := &Node{Name: name, GrpcPort: freePort(), HttpPort: freePort(), seed: seed}
	n.Start()
	DeferCleanup(n.Stop)
	return n
}

// Start launches the node process on its existing ports and waits until its
// HTTP API responds. Used for both the first start and restarts.
func (n *Node) Start() {
	args := []string{"-i", host, "-p", fmt.Sprint(n.GrpcPort), "-h", fmt.Sprint(n.HttpPort)}
	if n.seed != nil {
		args = append(args, "-seed", n.seed.Addr())
	}

	prefix := fmt.Sprintf("[%s] ", n.Name)
	out := gexec.NewPrefixedWriter(prefix, GinkgoWriter)

	var err error
	n.session, err = gexec.Start(exec.Command(dynagoBin, args...), out, out)
	Expect(err).NotTo(HaveOccurred())

	Eventually(func() error {
		resp, err := httpClient.Get(n.url("/ping"))
		if err != nil {
			return err
		}
		resp.Body.Close()
		return nil
	}).WithTimeout(10*time.Second).WithPolling(200*time.Millisecond).
		Should(Succeed(), "node %s did not start", n.Name)
}

// Stop sends SIGTERM and waits for the process to exit. Safe to call twice.
func (n *Node) Stop() {
	if n.session == nil {
		return
	}
	n.session.Terminate().Wait(10 * time.Second)
	n.session = nil
}

// Members returns the active members as reported by the node's /members API.
func (n *Node) Members() ([]string, error) {
	resp, err := httpClient.Get(n.url("/members"))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var members []string
	if err := json.NewDecoder(resp.Body).Decode(&members); err != nil {
		return nil, err
	}
	return members, nil
}

// Set stores a key/value through the node's HTTP API and returns the owner
// node that stored it, as reported in the response.
func (n *Node) Set(key, value string) (string, error) {
	body, _ := json.Marshal(map[string]string{"Key": key, "Value": value})
	resp, err := httpClient.Post(n.url("/kvstore"), "application/json", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("set %q via %s: status %d", key, n.Name, resp.StatusCode)
	}

	var result struct {
		Node string `json:"node"`
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	return result.Node, err
}

// GetResult is the JSON returned by GET /kvstore/:key.
type GetResult struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Node  string `json:"node"`
}

// Get reads a key through the node's HTTP API.
func (n *Node) Get(key string) (GetResult, error) {
	var result GetResult

	resp, err := httpClient.Get(n.url("/kvstore/" + key))
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return result, fmt.Errorf("get %q via %s: status %d", key, n.Name, resp.StatusCode)
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	return result, err
}

// freePort asks the OS for an unused TCP port.
func freePort() int {
	l, err := net.Listen("tcp", host+":0")
	Expect(err).NotTo(HaveOccurred())
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
