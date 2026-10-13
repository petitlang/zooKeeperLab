# ZooKeeper-like lab

Java 17 and gRPC implementation of the Lab3 coordination system.

## Current stage

Five independent localhost nodes can start and register both `GameService` and
`CoordinationService` on their respective ports. `LogStore` now persists proposals,
commit updates, and epochs; `NodeState` restores the committed target and manages
thread-safe runtime state. `GameService.Write/Read` and the coordination RPCs
`Propose`, `Commit`, `Forward`, `AnnounceLeader`, and `FetchLog` are implemented.
Writes replicate to a fixed quorum of 3, and followers forward writes to the
known leader without logging until a proposal arrives.

Automatic election, heartbeat monitoring, cross-epoch recovery/synchronization,
guess collection, and the command-line client are still pending. Their RPCs
currently return `UNIMPLEMENTED`. A temporary, explicit manual bootstrap lets
the normal write flow be exercised on a fresh cluster.

## Compile

Run commands from `D:\ZooKeeper\ZooKeeper` with JDK 17 or later and Maven:

```powershell
Set-Location 'D:\ZooKeeper\ZooKeeper'
mvn compile
```

## Start five nodes

Open five terminals in the project directory. For this development stage, start
the four followers first, then Node 1 with manual bootstrap enabled. Run one
command in each terminal:

```powershell
mvn exec:java "-Dexec.mainClass=fr.isep.node.NodeServer" "-Dexec.args=--id 2"
mvn exec:java "-Dexec.mainClass=fr.isep.node.NodeServer" "-Dexec.args=--id 3"
mvn exec:java "-Dexec.mainClass=fr.isep.node.NodeServer" "-Dexec.args=--id 4"
mvn exec:java "-Dexec.mainClass=fr.isep.node.NodeServer" "-Dexec.args=--id 5"
mvn exec:java "-Dexec.mainClass=fr.isep.node.NodeServer" "-Dexec.args=--id 1 --bootstrap-leader true"
```

| Node | Address | Data directory |
| --- | --- | --- |
| 1 | 127.0.0.1:5001 | data/node-1 |
| 2 | 127.0.0.1:5002 | data/node-2 |
| 3 | 127.0.0.1:5003 | data/node-3 |
| 4 | 127.0.0.1:5004 | data/node-4 |
| 5 | 127.0.0.1:5005 | data/node-5 |

Both services share each node's port. Each node knows all five configured
addresses and excludes itself from its four peers. The quorum is fixed at 3.
Each node opens its own `entries.log` on startup and restores its state. A restarted
node starts as a follower with no known leader and `ready=false`; persisted data
does not restore authority to act as leader.

Use Ctrl+C to stop a node. Orderly JVM shutdown runs the server shutdown hook;
forced process termination may bypass it. Leader-crash recovery requires the
election and replication implementation planned for later steps.

## Startup options

- `--id`: required node ID from 1 to 5.
- `--base-port`: default 5000. Listening port is base port + node ID. All nodes
  must use the same base port; the valid range is 0 through 65530.
- `--data-dir`: default `data`, resolved relative to the working directory.
  Each node gets a `node-<id>` subdirectory.
- `--bootstrap-leader`: default `false`. Set `true` only on the designated
  development leader. Bootstrap requires an epoch-zero empty local log, a fresh
  majority, and no existing history on responding peers. Start followers first.
  Existing logs must be retained for the future election/recovery implementation;
  manual bootstrap does not recover a previously used cluster.
- `--help`: print usage and exit.

Example using ports 6001 through 6005 and another data root:

```powershell
mvn exec:java "-Dexec.mainClass=fr.isep.node.NodeServer" "-Dexec.args=--id 1 --base-port 6000 --data-dir local-data"
```

For IntelliJ IDEA, run `fr.isep.node.NodeServer` with the project directory as the
working directory. Create five run configurations using `--id 2` through `--id 5`
for followers and `--id 1 --bootstrap-leader true` for the leader. Enable multiple
instances if using a single run configuration, and start followers first.

## Normal write flow

A Java gRPC client can call `GameService.Write` on any node with `client_id`, a
nonempty `request_id`, and `value` in 0 through 100. The generated blocking stub
can be used as follows after establishing a plaintext channel:

```java
WriteRequest request = WriteRequest.newBuilder()
        .setClientId("client-a").setRequestId("write-001").setValue(50).build();
OperationResponse result = GameServiceGrpc.newBlockingStub(channel)
        .withDeadlineAfter(5, TimeUnit.SECONDS).write(request);
GameState state = GameServiceGrpc.newBlockingStub(channel)
        .withDeadlineAfter(2, TimeUnit.SECONDS).read(ReadRequest.getDefaultInstance());
```

The leader serializes writes and durably logs a proposal, counting itself as one
acknowledgment. It contacts the four peers concurrently with a shared one-second
deadline per phase. Each follower checks the sending leader and epoch, saves the
proposal, then replies with its node ID and proposal tag. Only matching, accepted
ACKs from distinct peers count. A higher observed epoch prevents the old leader
from committing. At least two follower ACKs plus the local durable record are
required to commit. The leader persists its COMMIT and broadcasts COMMIT to all
four followers; followers persist it before exposing the target.

Missing quorum leaves the proposal `PROPOSED`, returns `REQUEST_UNCERTAIN`, and
keeps the visible target unchanged. Retry exactly the same client/request ID and
value. Retries reuse the pending tag; committed retries do not start another
round. A reused ID with changed data is rejected. New writes are blocked while
another proposal remains unresolved, preserving commit order.

Commit delivery has a deadline too. `REQUEST_COMMITTED` reports a durable leader
decision after quorum ACKs, not simultaneous application on every follower. In
this development stage, the next write replays the committed prefix to lagging
followers before sending its current proposal. Read returns local committed
state and can lag while a follower is unavailable or catching up. Same-epoch
catch-up is implemented; leader-failure recovery remains a separate next step.

Normal flow logs include `PROPOSE (epoch,seq)`, `ACK nodes=[...] quorum=3`, and
`COMMIT (epoch,seq)`. The integration tests exercise client writes through
followers, partial outages, pending-request retry, late follower catch-up,
mismatched ACKs, higher epochs, and concurrent writes.

## Persistent log and runtime state

The log is an append-only UTF-8 journal with tab-separated fields:

```text
EPOCH <current epoch>
ENTRY <epoch> <seq> <op> <value> <status> <client ID> <request ID> <resulting round>
```

The example uses spaces for readability; actual separators are tabs. Client and
request IDs are UTF-8 strings encoded with URL-safe Base64, so embedded tabs or
newlines cannot break a record. A commit appends the same entry with `COMMITTED`
status. Replay combines these records into one logical entry per tag.

Every mutation calls `FileChannel.force(true)` before returning. Duplicate tags
with identical proposal data and repeated commits are idempotent; conflicting
data, gaps, and commits ahead of earlier proposals are rejected. An incomplete
last line is truncated on restart. A malformed complete record fails startup
instead of silently dropping history. An exclusive file lock prevents two
instances from writing the same log.

`NodeState` changes its visible target only after a durable commit. Its synchronized
methods serialize proposal allocation and state changes. An immutable `GameState`
snapshot is used to read the target, round, role, and readiness together.

`LogEntry.round_id` identifies the round that starts when that entry commits;
`GuessRequest.round_id` identifies the existing round being guessed. A proposal
reserves the next round number, but the visible round advances only on commit.

New leader epochs are persisted before use and reset the sequence to zero. Leader
selection and cross-epoch resolution of pending proposals still require the
future election and synchronization algorithm. The coordinator now establishes
quorum for normal writes; storage/state methods alone do not establish quorum.

Run the recovery and concurrency tests:

```powershell
mvn test
```

## Windows selector compatibility

In the current automated execution environment, both installed JDKs failed
while initializing a Unix-domain socket used internally by the Windows NIO
selector (`Unable to establish loopback connection`, caused by
`Invalid argument: connect`). Five-node startup was verified with JDK 17 by
making the Unix-domain temporary directory unavailable, which causes the JDK
to fall back to TCP for this internal connection.

If the same error occurs, set the following only in the affected terminal before
running Maven. The indicated subdirectory must remain nonexistent:

```powershell
$env:JAVA_TOOL_OPTIONS = '-Djdk.net.unixdomain.tmpdir=D:\ZooKeeper\ZooKeeper\target\unused-unix-socket-directory'
```

This does not change the application's gRPC TCP protocol. To clear this temporary
setting, close the terminal or run:

```powershell
Remove-Item Env:JAVA_TOOL_OPTIONS
```
