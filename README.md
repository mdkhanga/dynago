# dynago

A distributed key-value store in Go, inspired by the
[Amazon Dynamo paper](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf).

## Status

Working today:
- Leaderless peer-to-peer cluster: every node is equal; new nodes join through any existing node (a seed)
- Membership via gossip, with failed nodes dropped after ~15 seconds
- Partitioning with consistent hashing (150 virtual nodes per server)
- In-memory get/set from any node; requests are forwarded to the key's owner

Not yet implemented:
- Replication (each key is stored on exactly one node)
- Read/write quorums, versioning and conflict resolution
- Hinted handoff, read repair, anti-entropy
- Durable storage, deletes

Known issue: nodes can disagree on a key's owner, because peers learned through
gossip are not added to the hash ring.

## Prerequisites

- Go 1.24+
- `protoc` with `protoc-gen-go` and `protoc-gen-go-grpc` (only needed to regenerate protobuf code)
- The Ginkgo CLI for unit tests: `go install github.com/onsi/ginkgo/v2/ginkgo@latest`

## Build

```bash
make          # regenerate protobuf code and build ./dynago
go build .    # build without regenerating protobuf code
```

## Run a cluster

Start the first server:
```bash
./dynago -i localhost
```

Start more servers, pointing each at any existing server with `-seed`:
```bash
./dynago -i localhost -p 8085 -h 8086 -seed localhost:8081
./dynago -i localhost -p 8087 -h 8088 -seed localhost:8081
```

| Flag | Meaning | Default |
|---|---|---|
| `-i` | IP address or hostname to bind to | `localhost` |
| `-p` | gRPC port for server-to-server traffic | `8081` |
| `-h` | HTTP port for clients | `8080` |
| `-seed` | `host:port` (gRPC port) of a server already in the cluster | none |

## Use it

Every request can go to any server.

```bash
# List cluster members
curl http://localhost:8080/members

# Store a key/value
curl -X POST -H "Content-Type: application/json" \
     -d '{"Key": "Name", "Value": "somevalue"}' \
     http://localhost:8080/kvstore

# Read it back (from any server)
curl http://localhost:8086/kvstore/Name
```

Writes and reads return the node that owns the key, e.g.
`{"key":"Name","node":"localhost:8085","value":"somevalue"}`.

## Tests

```bash
make test              # unit tests (Ginkgo)
go test -race ./...    # unit tests with the race detector
make e2e               # end-to-end tests: real multi-node clusters, ~3 minutes
```

The end-to-end tests in `e2e/` build the binary and start real nodes as separate
processes on free ports. Specs for known bugs are marked pending until they are fixed.

## License

Apache 2.0 — see [LICENSE](LICENSE).
