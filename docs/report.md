# ZooKeeper-like game — lab report

## Architecture

The project uses Java 17 and gRPC. Five node processes listen on ports 5001–5005.
One node acts as leader and the others follow it. A write needs three durable
copies, counting the leader, before it can commit.

NodeServer starts the process. NodeRuntime creates the server, peer connections,
election worker and log store, then closes them on shutdown. PeerTransport
handles node-to-node calls. GameEngine collects guesses; LeaderCoordinator
replicates and commits the chosen value. ElectionManager detects a missing leader,
and RecoveryManager synchronizes the logs before writes resume. NodeState keeps
the current role and committed game state; LogStore handles persistence.

Writes and recovery use the same coordinator lock. State and guess locks are
released before network calls. A guess settlement checks its original epoch and
round before creating a proposal, so a concurrent Write cannot commit a guess
against an obsolete target.

## Game rules

A round ends when two different clients have submitted guesses. Each client can
submit once per round; retrying the same request does not count twice. The value
with the smallest absolute distance from the target wins. Equal distances are
resolved by arrival order at the leader. The lab does not specify the collection
rule or tie-break, so these are implementation choices.

The winning guess is logged as GUESS and goes through PROPOSE, ACK and COMMIT.
The target and round change only after commit. A successful Write also starts a
new round and clears guesses collected for the previous target.

Collected guesses stay in memory until a winning proposal is created. They can
be lost if the leader stops. Persisted proposals are handled by retry or recovery.
The client distinguishes a collected guess (ACCEPTED), a committed operation
(COMMITTED), and an operation whose result needs checking (UNCERTAIN).

## Election and recovery

The heartbeat interval is 500 ms between maintenance ticks. Failure detection
uses elapsed monotonic time, a 2500 ms timeout and a 350 ms × node-ID stagger.
Candidates exchange their latest logged (epoch, seq), including proposed entries,
and fetch peer logs. The highest tag wins; equal tags favor the lowest node ID.
At least three nodes must respond.

The winner checks that the histories have compatible prefixes and preserves all
observed commits. Conflicting entries stop recovery. It persists an epoch greater
than every observed epoch, synchronizes its selected log to a majority, then
replicates and commits retained pending entries in order. Only then does it become
ready. New proposals use seq=1 in the new epoch. Periodic synchronization lets a
restarted follower catch up even when clients are idle.

Each node stores an append-only UTF-8 journal in data/node-<id>/entries.log.
ENTRY records contain epoch, seq, operation, value, status, client ID, request ID
and round. EPOCH stores the current term; SNAPSHOT stores a Base64 Protobuf
checkpoint. FileChannel.force(true) runs before acknowledging a durable record.
Restart replays the journal. An unfinished final line is truncated; a corrupt
complete record stops startup. A file lock prevents two processes from using the
same journal, and a checkpoint cannot remove or downgrade a commit.

## Test results

The suite passed 37 tests with no failures, errors or skipped tests. Results are
in [verification.txt](evidence/verification.txt) and [JUnit reports](evidence/junit/).
The [interactive run](evidence/manual-walkthrough/summary.txt) records the client
commands, node failures and retries.

| Test | Result |
| --- | --- |
| Write through a follower | Target 50 committed after majority ACKs |
| Two-client round | Guess 30 alone kept target 50; adding 62 committed target 62 |
| Leader stopped | A new leader retained 62 and accepted Write 40 |
| Original node restarted | It caught up without another client write |
| Guess after recovery | Guesses 39 and 80 committed target 39 |
| Only two nodes available | Target stayed 39; the same request committed 100 after a third node returned |
| Leader stopped with a local-only proposal | Surviving nodes retained committed target 50 |
| Leader stopped after majority proposal, before COMMIT | Recovery committed the retained value 62 |
| Leader stopped after partial COMMIT | Value 62 survived, and a later Write 70 succeeded |

The crash-stage tests use a separate leader fixture to pause the normal components
after local proposal, majority replication or partial commit. The test then kills
that JVM. The fixture is in the test sources; it is not a node startup option.

## Limits

Tests cover a fixed five-node localhost cluster with process crashes and restarts.
The implementation does not provide full ZAB or Raft guarantees for arbitrary
partitions, delays or membership changes. Read returns local committed state and
may lag on a follower. Guess counts are kept by the leader and are not replicated
before settlement. Quorum stays at three even when nodes stop.

Startup commands and the three demonstration flows are in [README](../README.md).
