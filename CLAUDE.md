# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Dynago is a distributed key-value store inspired by Amazon's Dynamo paper, implementing a leaderless peer-to-peer cluster architecture with consistent hashing and eventual consistency. Written in Go, it uses gRPC for server-to-server communication and HTTP/REST for client operations.

## Building and Running

### Build
```bash
make
```

This runs `make proto` to generate protobuf code from `kvmessages/messages.proto`, then builds the `dynago` binary.

### Regenerate Protocol Buffers
```bash
make proto
```

Generates Go code and gRPC stubs from `kvmessages/messages.proto`. Required after modifying the proto file.

### Run Tests
```bash
make test
```

Uses Ginkgo test framework. The project uses Ginkgo v2 and Gomega for testing.

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
- `-i`: IP address to bind to (required)
- `-p`: gRPC port for peer-to-peer communication (default: 8081)
- `-h`: HTTP port for client API (default: 8080)
- `-seed`: Address of existing cluster member to join (format: `host:port`)

## Architecture

### Core Components

**Server Layer** (`server/server.go`):
- Entry point that coordinates HTTP and gRPC servers
- Manages lifecycle of cluster service, HTTP API (Gin), and gRPC server
- HTTP endpoints: `/ping`, `/kvstore`, `/kvstore/:key`, `/members`

**Cluster Management** (`cluster/`):
- `ClusterService`: Singleton managing cluster membership via gossip protocol
- Runs periodic gossip every 1 second to share cluster state
- Marks peers inactive after 15 seconds without updates
- `Peer`: Represents a connection to another node with bidirectional streaming
- Each peer runs 4 goroutines: receiveLoop, sendLoop, processMessageLoop, pingLoop

**Peer Communication Pattern**:
- Bidirectional gRPC streams for all peer-to-peer communication
- Two peer instances per connection: client-side (`Clientend=false`) and server-side (`Clientend=true`)
- Client-side peers run ping loops; server-side peers respond
- Messages flow through buffered channels: `InMessagesChan` and `OutMessagesChan` (capacity: 100)

**Stream Abstraction** (`cluster/streams.go`):
- `IStream` interface unifies client and server gRPC streams
- `ClientStream` wraps `grpc.BidiStreamingClient`
- `ServerStream` wraps `pb.KVSevice_CommunicateServer`

**Message Protocol** (`kvmessages/messages.proto`):
- Protobuf-based communication
- Message types: `PING`, `PING_RESPONSE`, `CLUSTER_INFO_REQUEST`, `CLUSTER_INFO_RESPONSE`, `KEY_VALUE`
- All peer messages use `ServerMessage` wrapper with oneof content

**Storage** (`storage/memerystore.go`):
- Global singleton `Store` with in-memory map
- Thread-safe via mutex locking
- Simple Get/Set operations for KeyValue pairs

**Consistent Hashing** (`consistenthash/consistenthash.go`):
- Virtual node-based consistent hashing
- Not yet integrated into main replication logic

**Configuration** (`config/config.go`):
- Singleton initialized at startup with hostname and ports
- Used throughout cluster for identifying local node

### Request Flow

**Write Operation**:
1. Client POSTs to `/kvstore` with JSON `{"Key": "...", "Value": "..."}`
2. Server stores locally in `storage.Store`
3. `ClusterService.Replicate()` sends `KEY_VALUE` message to all active peers
4. Remote peers receive via `processMessageLoop` and store locally

**Read Operation**:
1. Client GETs from `/kvstore/:key`
2. Server reads from local `storage.Store` only
3. Returns value (replication still in progress, reads are not distributed yet)

**Cluster Join**:
1. New node connects to seed via gRPC `Communicate()` stream
2. Seed creates server-side peer, new node creates client-side peer
3. Client sends periodic `PING` messages with hostname/port
4. Server responds with `PING_RESPONSE` and adds peer to cluster
5. Gossip protocol spreads membership to all nodes

**Gossip Protocol**:
1. Every second, each node sends `CLUSTER_INFO_REQUEST` to all connected peers
2. Message contains full membership list with timestamps
3. Recipients merge received list with local state (last-write-wins on timestamp)
4. If recipient has newer info, responds with `CLUSTER_INFO_RESPONSE`
5. Nodes mark peers inactive if no update received for 15 seconds

### Key Packages

- `main.go`: Parses flags, initializes server
- `server/`: HTTP and gRPC server management
- `cluster/`: Peer-to-peer clustering, gossip, membership
- `grpcserver/`: gRPC service implementation (server-side of streams)
- `grpcclient/`: gRPC client for initiating connections to seed nodes
- `storage/`: In-memory key-value storage
- `config/`: Global configuration singleton
- `kvmessages/`: Generated protobuf code
- `consistenthash/`: Consistent hashing (not yet used in replication)
- `logger/`: Zerolog-based structured logging
- `models/`: Data models (KeyValue struct)
- `utils/`: Helper functions (string parsing, etc.)

## Development Notes

### Current State
- Cluster formation and membership gossip: **Working**
- Local key-value storage: **Working**
- Replication to peers: **Working** (fire-and-forget, no quorum)
- Read operations: Only read from local node (distributed reads not implemented)
- Consistent hashing: Implemented but not integrated
- Write quorums: Not implemented
- Read quorums: Not implemented
- Anti-entropy/conflict resolution: Not implemented

### Testing
The project uses Ginkgo/Gomega for tests. Tests are located in `e2e-tests/` directory.

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
