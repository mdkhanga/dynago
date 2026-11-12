# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get

# Name of the binary executable
BINARY_NAME=dynago

# Main build target
all: build

build: proto
	$(GOBUILD) -o $(BINARY_NAME) -v

clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME)

run:
	$(GOBUILD) -o $(BINARY_NAME) -v
	./$(BINARY_NAME)

test:
	ginkgo -r --skip-package=e2e

# End-to-end tests: build the binary and run real multi-node clusters
e2e:
	$(GOTEST) -tags e2e -count=1 -v ./e2e/...

deps:
	$(GOGET) github.com/example/dependency

.PHONY: all build clean run test e2e deps

proto: 
	protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    kvmessages/messages.proto
