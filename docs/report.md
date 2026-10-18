# ZooKeeper-like distributed game: implementation report

Date: 18 October 2026. Baseline: main at 52a3996.

## Design

Five independent Java 17 processes expose GameService and CoordinationService on
ports 5001–5005. Protocol Buffers provide the shared wire contract. The fixed
majority is three nodes, including the leader's durable local copy. The game
application exercises the coordination layer rather than bypassing it.

NodeRuntime owns construction and resource disposal. NodeServer remains the
process entrypoint. PeerTransport owns channels and bounded concurrent calls.
GameEngine owns game rules. LeaderCoordinator owns serial commit decisions;
ElectionManager owns failure detection; RecoveryManager owns history selection
and synchronization. NodeState owns atomic transitions, while LogStore owns disk
format, validation and recovery. RPC adapters delegate to these components.
No mutable collection, executor or channel is exposed by getters.

State locks cover validation and local changes only, never remote calls. The
coordinator operation monitor serializes normal writes and recovery. Guess
settlement releases its collection lock before replication and rechecks the
captured epoch and round before allocating a proposal. A stale settlement callback
cannot clear a newer round's settling flag.

## Game rules chosen where the specification leaves details open

A round ends after two different client IDs submit valid guesses. There is no
collection timer. The initial Write does not count as a guess. Each client may
submit once per round; an identical request retry does not increase the count.
The smallest absolute distance wins, with arrival order breaking ties. The
winning guess follows the same proposal, durable ACK and COMMIT protocol as Write.
Only a committed result advances the round. A successful Write resets the target
and invalidates uncommitted guesses from the former round.

Incomplete collections remain in leader memory, so a leader failure may lose
them. Once a winning proposal has been persisted, it is resolved by retry or
recovery using its original identity. The two states are intentionally reported
as ACCEPTED versus COMMITTED; an uncertain proposal is reported as UNCERTAIN.

## Election and durable recovery

The failure detector uses monotonic time, a 2500 ms timeout and a node-ID stagger.
Candidates exchange tags and fetch histories from peers. The greatest logged
(epoch, seq), including PROPOSED entries, wins; equal tags favor the lowest ID.
At least three responding nodes are required.

Recovery validates compatible prefixes and preserves observed commits. It fails
closed on conflicting histories. The new epoch is greater than all epochs
observed and is durable before use. The selected history is synchronized to a
majority; retained pending entries are durably replicated again before being
committed in order. The node becomes ready only after recovery succeeds. New
writes use sequence 1 in the new epoch. Restarted followers are repaired by
periodic synchronization, even if no additional client writes occur.

Recovery checkpoints are append-only SNAPSHOT records containing Base64 Protobuf.
The complete record is forced to disk before memory is changed. Existing EPOCH
and ENTRY records are still supported. A checkpoint cannot remove or downgrade a
commit. A torn final record is truncated on restart, while complete corrupt data
stops startup. File locking prevents simultaneous writers to one node's journal.

## Demonstrated behavior

Retained scenario outputs are in [evidence](evidence/). Full reproducible commands
are in the repository README.

| Scenario | Observation |
| --- | --- |
| Normal write through follower | Target 50 committed using majority ACKs |
| Fixed two-client guessing | 30 alone does not settle; adding 62 commits 62 and advances the round |
| Leader crash | A follower becomes leader, keeps target 62 and accepts a new Write of 40 |
| Rejoin | The killed node restarts with its original journal and catches up without another write |
| Game after recovery | New guesses commit 39 through the rejoined node |
| Quorum absent | With 2/5 nodes, target stays 39; retrying the same ID after 3/5 return commits 100 |
| Crash with local-only proposal | Other nodes keep the committed target 50; the unreplicated 62 need not survive |
| Crash after majority proposal, before COMMIT | Recovered history commits the retained value 62 |
| Crash after partial COMMIT | Value 62 is preserved, then a new write of 70 succeeds |

The crash-boundary tests use a test-only driver with production state, storage,
transport, RPC handlers and coordinator. The driver pauses at an exact boundary;
the parent forcibly kills that JVM. Production code has no crash-injection flags.

## Verification and limits

The verified run completed **37 tests with 0 failures, 0 errors and 0 skipped**.
See [verification summary](evidence/verification.txt) and [JUnit reports](evidence/junit/).

The automated suite covers existing storage/state/replication behavior, checkpoint
replay, conflict rejection, two-client admission, tie-breaking, zero and 100,
obsolete rounds, concurrent Write/Guess, retry, process resource cleanup, and
independent-process election/recovery. Process traces and JUnit summaries are
retained with the report.

The model is the lab's fixed localhost cluster with process crashes and restarts.
It is not full ZAB or Raft and makes no general guarantee for arbitrary network
partitions or unbounded delays. Reads are local committed snapshots and can be
stale. Follower Read does not expose the leader's volatile guess collection;
Guess responses and leader Read provide that progress. Quorum remains 3/5 even
when nodes are down. Minority operation sacrifices write availability.

## Live defence

Start five node terminals, then clients A, B and C on different nodes. Show
`write 50`, `guess 30`, `guess 62`, and the corresponding PROPOSE/ACK/COMMIT logs.
Identify the actual leader using Read, stop that process, wait for a new ready
leader, then demonstrate another Write and another two-client round. Restart the
old node without deleting its data and inspect its committed state. All three
flows must be available because the examiner may choose any one.
