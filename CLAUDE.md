# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Dynago is a distributed key-value store inspired by Amazon's Dynamo paper, implementing a leaderless peer-to-peer cluster architecture with consistent hashing. Written in Go, it uses gRPC for server-to-server communication and HTTP/REST for client operations.

## Building and Running

### Prerequisites
- Go 1.24+ (`go.mod` pins toolchain `go1.24.4`; older Go downloads it automatically). Go 1.22 test binaries crash on current macOS with "missing LC_UUID".
- `protoc`, `protoc-gen-go`, `protoc-gen-go-grpc` (current generated code: protoc 27.3, protoc-gen-go v1.34.2, protoc-gen-go-grpc v1.5.1)

### Build
```bash
make
```

This runs `make proto` to generate protobuf code from `kvmessages/messages.proto`, then builds the `dynago` binary. `go build .` builds without regenerating.

### Regenerate Protocol Buffers
```bash
make proto
```

Generates Go code and gRPC stubs from `kvmessages/messages.proto`. Required after modifying the proto file.

### Run Tests
```bash
make test              # ginkgo -r
go test -race ./...    # same unit tests, with the race detector
```

Unit tests use Ginkgo v2 and Gomega and live next to the code (`cluster/`, `storage/`).

End-to-end tests are Python scripts in `e2e-tests/` that launch `./dynago` processes on localhost and drive them over HTTP. Build the binary first, then run from the repo root (requires the `requests` package):
```bash
python3 e2e-tests/test_key_distribution.py
```

### Running Servers

First server (seed node):
```bash
./dynago -i <ip_address>
```

Additional servers:
```bash
./dynago -i <ip_address> -p 8085 -h 8086 -seed <seed_ip>:8081
```

Flags:
- `-i`: IP address to bind to (default: `localhost`)
- `-p`: gRPC port for peer-to-peer communication (default: 8081)
- `-h`: HTTP port for client API (default: 8080)
- `-seed`: Address of existing cluster member to join (format: `host:port`)

## Architecture

### Core Components

**Server Layer** (`server/server.go`):
- Entry point that coordinates HTTP and gRPC servers
- Manages lifecycle of cluster service, HTTP API (Gin), and gRPC server
- HTTP endpoints: `/ping`, `/kvstore`, `/kvstore/:key`, `/members`
- Routes each GET/POST to the key's owner node via the hash ring

**Cluster Management** (`cluster/`):
- `ClusterService`: Singleton managing cluster membership via gossip protocol
- Runs periodic gossip every 1 second to share cluster state
- Marks peers inactive after 15 seconds without updates
- `ForwardGet` / `ForwardSet`: send a request to the owner node's peer and wait (5s timeout) for the response
- `Peer`: Represents a connection to another node with bidirectional streaming
- Each peer runs goroutines: receiveLoop, sendLoop, processMessageLoop, and (dialing side only) pingLoop

**Peer Communication Pattern**:
- One bidirectional gRPC stream (`Communicate`) carries all peer-to-peer messages
- Two peer instances per connection: the dialing side (`Clientend=false`, runs the ping loop) and the accepting side (`Clientend=true`, created in `grpcserver`). Note: the field comment in `peer.go` describes this backwards.
- Messages flow through buffered channels: `InMessagesChan` and `OutMessagesChan` (capacity: 100)
- Forwarded responses are delivered through per-peer `getResponseChan` / `setResponseChan` (capacity 1, no request IDs)

**Stream Abstraction** (`cluster/streams.go`):
- `IStream` interface unifies client and server gRPC streams
- `ClientStream` wraps `grpc.BidiStreamingClient`
- `ServerStream` wraps `pb.KVSevice_CommunicateServer`

**Message Protocol** (`kvmessages/messages.proto`):
- Protobuf-based communication
- Message types: `PING`, `PING_RESPONSE`, `KEY_VALUE`, `CLUSTER_INFO_REQUEST`, `CLUSTER_INFO_RESPONSE`, `FORWARD_GET_REQUEST`, `FORWARD_GET_RESPONSE`, `FORWARD_SET_REQUEST`, `FORWARD_SET_RESPONSE`
- All peer messages use `ServerMessage` wrapper with oneof content

**Storage** (`storage/memerystore.go`):
- Global singleton `Store` with in-memory map
- Thread-safe via mutex locking
- Simple Get/Set operations for KeyValue pairs (a missing key returns an empty value)

**Consistent Hashing** (`consistenthash/hashring.go`):
- Virtual node-based ring (150 virtual nodes per node, SHA-256 truncated to 32 bits)
- Global `cluster.HashRing`; nodes are added in `AddToCluster` and removed in `RemoveFromCluster`
- Used to pick the single owner node for each key

**Configuration** (`config/config.go`):
- Singleton initialized at startup with hostname and ports
- Used throughout cluster for identifying local node

### Request Flow

**Write Operation**:
1. Client POSTs to `/kvstore` with JSON `{"Key": "...", "Value": "..."}`
2. Server looks up the owner node with `ClusterService.GetNodeForKey()`
3. If this node is the owner, it stores in `storage.Store`
4. Otherwise it calls `ForwardSet()`, which sends `FORWARD_SET_REQUEST` to the owner and waits for `FORWARD_SET_RESPONSE`
5. Each key is stored on exactly one node (no replication). `ClusterService.Replicate()` exists but is not called.

**Read Operation**:
1. Client GETs from `/kvstore/:key`
2. Server looks up the owner node; reads locally if it is the owner
3. Otherwise `ForwardGet()` sends `FORWARD_GET_REQUEST` and waits for `FORWARD_GET_RESPONSE`
4. Returns 200 with `{"key", "value", "node"}`; a missing key returns an empty value, not 404

**Cluster Join**:
1. New node connects to seed via gRPC `Communicate()` stream
2. Seed creates the accepting-side peer, new node creates the dialing-side peer
3. Dialing side sends periodic `PING` messages with hostname/port
4. Accepting side responds with `PING_RESPONSE` and adds the peer to the cluster
5. Gossip spreads membership; nodes learned via gossip are dialed with `Peer.Init()`

**Gossip Protocol**:
1. Every second, each node sends `CLUSTER_INFO_REQUEST` to the active peers on connections it accepted (`Clientend=true`)
2. Message contains full membership list with wall-clock timestamps
3. Recipients merge received list with local state (newer timestamp wins)
4. Recipient responds with `CLUSTER_INFO_RESPONSE` containing entries the sender is missing or has older
5. Nodes mark peers inactive if no update received for 15 seconds

### Key Packages

- `main.go`: Parses flags, initializes server
- `server/`: HTTP and gRPC server management, request routing
- `cluster/`: Peer-to-peer clustering, gossip, membership, request forwarding
- `grpcserver/`: gRPC service implementation (accepting side of streams)
- `grpcclient/`: gRPC client for connecting to the seed node
- `storage/`: In-memory key-value storage
- `config/`: Global configuration singleton
- `kvmessages/`: Protobuf definition and generated code
- `consistenthash/`: Consistent hash ring
- `logger/`: Zerolog-based structured logging
- `models/`: Data models (KeyValue struct)
- `utils/`: Helper functions (string parsing)

## Development Notes

### Current State
- Cluster formation and membership gossip: **Working**
- Key routing via consistent hashing: **Working**, but see the ring-drift issue below
- Local key-value storage (in memory): **Working**
- Replication: Not implemented (one copy per key)
- Read/write quorums: Not implemented
- Versioning/conflict resolution: Not implemented
- Read repair, hinted handoff, anti-entropy, durable storage, deletes: Not implemented

### Known Issues
- **Hash rings drift between nodes**: peers learned via gossip (`MergePeerLists`) are added to the cluster map but never to `HashRing`, and peers marked inactive by gossip are not removed from it. Different nodes can therefore route the same key to different owners.
- **Data races** on `Peer` fields (`Status`, `Timestamp`, `Host`, `Port`) between peer goroutines and `ClusterService`; `go run -race` reports several per node.
- **Forwarded responses can be mismatched**: each peer has a single response channel with no request IDs, so concurrent forwards to the same node can receive each other's responses.
- **Blocking under lock**: `ClusterInfoGossip` and `Replicate` send to `OutMessagesChan` while holding the cluster mutex; a stuck peer with a full channel blocks the cluster.
- Liveness uses wall-clock timestamps compared across machines (sensitive to clock skew).
- HTTP API: missing keys return 200 with an empty value; `BindJSON` errors are ignored; no DELETE; Gin runs in debug mode; no SIGTERM handling.

### Direction
Phase 1 of the roadmap replaces the bidirectional stream with unary gRPC RPCs (Get/Put/Delete/Gossip; server-streaming only for bulk transfers), separates membership (heartbeat counters, alive/suspect/dead) from connections, derives the ring purely from alive members, adds versioned last-write-wins (HLC timestamp + node ID), and removes global singletons. Avoid building new features on the current stream/`Peer` code.

### Logging
Uses `rs/zerolog` with component-based loggers. Each package creates its own logger via `logger.WithComponent()`.

### Concurrency Patterns
- Heavy use of channels for message passing between goroutines
- Mutex locking for shared state (cluster map, peer state, storage)
- Each peer spawns multiple goroutines that must be properly cleaned up via `stopChan`
- `sync.Once` used to ensure single cleanup of resources

### Protocol Buffer Changes
After modifying `kvmessages/messages.proto`:
1. Run `make proto` to regenerate Go code
2. Update message handlers in `cluster/peer.go` `processMessageLoop()`
3. Update message senders where needed
