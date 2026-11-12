//go:build e2e

package e2e

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

// dynagoBin is the path to the dynago binary built once for the whole suite.
var dynagoBin string

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "E2E Suite")
}

var _ = BeforeSuite(func() {
	var err error
	dynagoBin, err = gexec.Build("github.com/mdkhanga/dynago")
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	gexec.KillAndWait()
	gexec.CleanupBuildArtifacts()
})
