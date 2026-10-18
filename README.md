# ZooKeeper-like lab

Java 17, Maven, gRPC and Protocol Buffers. Five independent localhost processes,
one elected leader, four followers and a fixed quorum of **3/5**.

## Build and run

In PowerShell, with Java 17+ installed:

```powershell
Set-Location 'D:\zooKeeperLab'
./scripts/build.ps1
```

The scripts find Maven on PATH or inside IntelliJ IDEA. If execution policy blocks
local scripts, use `Set-ExecutionPolicy -Scope Process Bypass` in this terminal.
The Windows JDK selector workaround is applied only to launched Java/Maven
processes. Its `target/nonexistent-selector-dir` directory must stay nonexistent.

Open five terminals in the project, one command per terminal:

```powershell
./scripts/run-node.ps1 -Id 1
./scripts/run-node.ps1 -Id 2
./scripts/run-node.ps1 -Id 3
./scripts/run-node.ps1 -Id 4
./scripts/run-node.ps1 -Id 5
```

Ports are 5001–5005; logs are `data/node-<id>/entries.log`. Startup order is free.
Wait for `LEADER ready` and confirm `ready=true` using a client. On fresh logs,
equal tags favor node 1; with existing logs the result depends on history.
Use the same `-BasePort 6000` on all node/client scripts to change ports, or
`-DataDir demo-data` on all nodes to isolate a new demonstration.
**Retain the same data directory when restarting a crashed node.**

Without scripts, use `mvn compile`, then
`mvn exec:java "-Dexec.mainClass=fr.isep.node.NodeServer" "-Dexec.args=--id 1"`.
In IntelliJ, import `pom.xml` and run `fr.isep.node.NodeServer` five times with
`--id 1` through `--id 5`, with this project as working directory.
The optional `--bootstrap-leader true` remains a fresh-log development tool;
normal demonstrations use automatic election and do not need this option.

## Clients and three demonstrations

Open three clients in separate terminals:

```powershell
./scripts/run-client.ps1 -Node 4 -Client A
./scripts/run-client.ps1 -Node 3 -Client B
./scripts/run-client.ps1 -Node 5 -Client C
```

Commands: `write N`, `guess N`, `read`, `retry`, `connect NODE`, `help`, `quit`.
Client IDs must be distinct. Each new write/guess gets a UUID; `retry` preserves
the exact ID, value and original guess round. A new command replaces the retained
request. No automatic write retry or node switching is performed.

1. **Write:** A enters `write 50`, then `read`. A follower forwards without
   logging the client request. Leader output shows PROPOSE, ACK and COMMIT.
   The leader counts its own durable record plus at least two distinct follower
   ACKs. Target/round change only after durable COMMIT.
2. **Guess:** B enters `guess 30`: response is ACCEPTED, progress 1/2 and target
   stays 50. C enters `guess 62`: the second distinct client triggers settlement.
   Distances are 20 and 12, so 62 is committed as the new target and a new round
   starts. There is **no time window**. One guess per client per round; ties favor
   arrival order at the leader. Successful `write` invalidates old-round guesses.
3. **Leader crash:** read `leader=...`, then kill that node's Java process or
   stop its terminal. Wait for another `LEADER ready` (typically several seconds;
   check readiness rather than relying on a fixed sleep). A living follower can
   continue `write` and `guess`. A client attached to the killed node uses
   `connect NODE` to select a living node. Restart the original node with the
   same ID and data directory; it catches up without needing another write.

Noninteractive inspection:

```powershell
1..5 | ForEach-Object { ./scripts/run-client.ps1 -Node $_ -Client inspect -Command 'read' }
```

Read is local and may be stale; it reports node, leader, role, ready, target,
round and last committed tag. Guess progress is maintained by the leader:
forwarded Guess responses show the leader's progress; follower local Read does
not reconstruct the leader's volatile collection.

## Encapsulation and concurrency

| Component | Responsibility |
| --- | --- |
| NodeServer / NodeRuntime | Process entrypoint / composition and lifecycle |
| NodeConfig | Immutable addresses, quorum, timeouts, required guesses=2 |
| PeerTransport | Reused channels, bounded concurrent peer RPCs, forwarding |
| GameServiceImpl / CoordinationServiceImpl | Thin client / inter-node RPC adapters |
| GameEngine | Admission, per-client guesses, round validation, winner selection |
| LeaderCoordinator | Serialized proposal allocation, quorum replication, commit |
| ElectionManager | Heartbeat and monotonic failure timeout |
| RecoveryManager | Candidate comparison, reconciliation, synchronization, catch-up |
| NodeState | Atomic role/epoch/sequence/committed-state transitions |
| LogStore | Durable records, validation, checkpoints, replay and file locking |
| ClientApp | Explicit commands, retained request and connection lifecycle |

Only NodeRuntime owns long-lived server, worker, channel and log resources.
Shutdown is idempotent; partial startup failures release resources. Collections
are private and snapshots immutable. Writes and recovery share the coordinator's
operation monitor. State and guess locks are never held while waiting for network
calls. A settlement checks its captured epoch/round again inside the coordinator.

Peer phases have a shared one-second deadline and run concurrently. Forwarding
has a five-second deadline; the client uses six seconds. Heartbeats run every
500 ms between completed maintenance ticks. Failure detection uses monotonic
elapsed time, a 2500 ms timeout and a 350 ms × node-ID election stagger.
Unavailable peers and catch-up can lengthen the interval between heartbeats.

## Election and recovery

Candidates exchange latest tags, including PROPOSED entries, and fetch logs.
The greatest `(epoch, seq)` wins; equal tags favor the smallest node ID. At least
three responses are required. The winner checks compatible prefix histories and
merges observed commit knowledge. Conflicting histories stop recovery with a
clear error instead of silently choosing a value.

The next epoch exceeds all observed/current epochs and is persisted before use.
The elected node stays unready while its selected history is durably synchronized
to a majority. Retained pending proposals keep their original tags, are copied to
a majority again, then committed in order. New proposals in the new epoch start
at seq=1. Recovery/catch-up checkpoints may contain old tags; normal PROPOSE
handlers still require the current epoch. Periodic maintenance repairs restarted
or lagging followers. A snapshot cannot delete or downgrade a local commit.

`REQUEST_COMMITTED` means a durable decision after majority replication; followers
need not apply it simultaneously. `REQUEST_ACCEPTED` means a guess was collected,
not committed. `REQUEST_UNCERTAIN` means read/retry is needed; a pending proposal
can become committed during recovery. Retry the same ID and data. Changing data
under an existing ID is rejected. A pending write blocks later writes.

Queued guesses that have not produced a durable proposal are volatile and may be
lost with the leader. Read the recovered round, then resubmit. A local-only
proposal that never reached a majority may be discarded when another history
is selected. Committed history must always be preserved.

## Persistence

UTF-8 append-only records: `EPOCH`, `ENTRY` and `SNAPSHOT`. Existing EPOCH/ENTRY
journals remain readable. Entries contain epoch, seq, WRITE/GUESS, value,
PROPOSED/COMMITTED, client ID, request ID and resulting round. IDs are URL-safe
Base64; SNAPSHOT stores a complete Protobuf checkpoint as Base64.

A checkpoint is validated and fully written with `FileChannel.force(true)` before
changing memory or acknowledging it. An unfinished final record is truncated on
restart; a corrupt complete record fails startup. An exclusive file lock prevents
two writers. Restart restores data, never previous leader authority.

## Tests and evidence

```powershell
./scripts/test.ps1
```

Unit/integration tests cover storage, checkpoints, state, duplicate requests,
quorum loss/retry, ACK validation, simultaneous writes, game rules and lifecycle.
Legacy replication tests explicitly disable background election to exercise their
manual-bootstrap protocol scenarios without concurrent recovery.

`ClusterProcessTest` launches five independent JVMs, uses forced process termination,
and covers all three demonstrations, old-node rejoin and absent-quorum retry.
A test-only leader driver also pauses production components at three deterministic
boundaries: local PROPOSED only, proposal durable on 3/5 before COMMIT, and COMMIT
stored on leader plus one follower. The parent forcibly kills it, then checks
re-election and continuation. There are no production crash-injection switches.
Tests use temporary data and reserved port ranges, separate from demonstration logs.

JUnit results: `target/surefire-reports/`. Live process traces and scenario evidence:
`target/demo-evidence/run-*/`. Retained evidence and the report are under `docs/`.

## Model limits

This is the lab's simplified fixed-five-node protocol on a stable localhost
network, tested for process stop/restart. It is not complete ZAB or Raft and does
not promise consensus under arbitrary partitions, arbitrary delays, Byzantine
nodes or membership changes. A majority is always 3/5, even after nodes stop.
