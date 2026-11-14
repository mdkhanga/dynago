# Dynago plan

Where dynago is going, in order. Each step lands as one or more small commits,
with unit tests (`go test -race`) and end-to-end tests (`make e2e`) passing.

## Phase 0: Hygiene — almost done

- [x] Go 1.24 toolchain
- [x] Remove dead code (old TCP implementation)
- [x] Build, vet and unit tests green
- [x] `.gitignore`, protobuf `go_package`
- [x] End-to-end tests ported from Python to Go/Ginkgo (`make e2e`)
- [x] CLAUDE.md, README
- [ ] CI: run `make test` and `make e2e` on every push

## Phase 1: Foundation rebuild

The current design runs all peer traffic over one custom bidirectional gRPC stream
per connection, with hand-rolled message routing. That causes mismatched forwarded
responses, data races and nodes that disagree about the hash ring. Phase 1 replaces it.

1. **Remove global singletons.** A `Node` struct owns its config, cluster, ring and
   store, so several nodes can run in one test process.
2. **Membership separate from connections.** Stable node IDs; heartbeat counters
   instead of wall-clock timestamps; alive → suspect → dead states.
3. **Ring derived from membership.** The hash ring is a pure function of the alive
   members, so every node with the same membership picks the same owner.
   (Un-pends the "same owner from every node" e2e spec.)
4. **Unary RPCs.** `Get`, `Put`, `Delete` and `Gossip` as plain gRPC calls with
   deadlines; server streaming only for bulk transfer. Removes the `Communicate`
   stream and per-peer message loops.
5. **Versioned values.** Each value carries a version (hybrid logical clock +
   node ID); conflicts resolve by last-write-wins. Behind a `Version` type so
   vector clocks can replace it later.
6. **HTTP API cleanup.** 404 for missing keys, 400 for bad JSON, `DELETE` (tombstones),
   Gin release mode, graceful shutdown on SIGTERM.

## Phase 2: Dynamo features

1. **Replication.** Each key stored on N nodes: the first N distinct nodes clockwise
   on the ring (the preference list).
2. **Quorums.** Configurable N, R, W; the coordinator waits for R reads / W writes.
3. **Read repair.** Stale replicas found during a read are updated.
4. **Hinted handoff.** Writes for a down replica go to a stand-in node and are
   replayed when it returns.
5. **Rebalancing.** Keys move to their new owners when nodes join or leave.
6. **Anti-entropy.** Replicas compare Merkle trees and sync the ranges that differ.

## Phase 3: Production readiness

- Durable storage
- Metrics and health endpoints
- Configuration file, container image, multi-node local setup
- Fault-injection tests (partitions, crashes, clock skew)
