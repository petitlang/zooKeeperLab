# ZooKeeper-like System

This project is a distributed number game for the ZooKeeper lab.
It uses Java 17, Maven, gRPC, and Protobuf.
The cluster runs five separate node processes on localhost.
One node is the Leader. The other nodes are Followers.
A commit needs a majority of **3 out of 5 nodes**.

## 1. Requirements

- Windows with PowerShell for the scripts below.
- JDK 17 or later, with `java` available on PATH.
- Maven on PATH, or Maven bundled with IntelliJ IDEA.
- An internet connection for the first build.
- Five free ports: `5001` to `5005` by default.

Check Java:

```powershell
java -version
```

Run the commands from the project folder.
The examples use `D:\zooKeeperLab`. Change this path if needed.

```powershell
Set-Location 'D:\zooKeeperLab'
```

If PowerShell blocks a script, run this in that terminal, then try again:

```powershell
Set-ExecutionPolicy -Scope Process Bypass
```

This setting lasts only for the current PowerShell session.

## 2. Build the project

```powershell
./scripts/build.ps1
```

The script compiles the code, generates the Protobuf classes, and builds the project.
It also writes the runtime classpath to `target/runtime-classpath.txt`.
Wait for `BUILD SUCCESS` before starting nodes.
Run the build again after changing the source code.

## 3. Start five nodes

Open five PowerShell terminals in the project folder.
Run **one command in each terminal**:

| Terminal | Command | Port | Local log |
| --- | --- | --- | --- |
| Node 1 | `./scripts/run-node.ps1 -Id 1` | 5001 | `data/node-1/entries.log` |
| Node 2 | `./scripts/run-node.ps1 -Id 2` | 5002 | `data/node-2/entries.log` |
| Node 3 | `./scripts/run-node.ps1 -Id 3` | 5003 | `data/node-3/entries.log` |
| Node 4 | `./scripts/run-node.ps1 -Id 4` | 5004 | `data/node-4/entries.log` |
| Node 5 | `./scripts/run-node.ps1 -Id 5` | 5005 | `data/node-5/entries.log` |

The nodes elect a Leader automatically. Do not select a Leader manually.
Wait until a node prints `LEADER ready`.
Then use a client to check that `ready=true`.
At least three nodes must be available before the cluster can become ready.

With empty logs and equal candidate tags, the smallest node ID wins.
With old logs, the Leader depends on the log history.
Always read `leader=...` instead of assuming that node 1 is the Leader.

### Other ports or a separate demo directory

To use ports `6001` to `6005`, add `-BasePort 6000` to **every node and client command**.
To keep a new run separate from old data, add `-DataDir demo-data` to **every node command**.
For example:

```powershell
./scripts/run-node.ps1 -Id 1 -BasePort 6000 -DataDir demo-data
```

Start nodes 2 to 5 in the same way, with their own IDs.
Use the same data directory and base port when restarting a node.
To start a fresh game later, choose a new data directory for all five nodes.

## 4. Start clients

Keep the node terminals open. Open three more PowerShell terminals in the project folder.
Run one client in each terminal:

```powershell
# Client A connects to node 4.
./scripts/run-client.ps1 -Node 4 -Client A
```

```powershell
# Client B connects to node 3.
./scripts/run-client.ps1 -Node 3 -Client B
```

```powershell
# Client C connects to node 5.
./scripts/run-client.ps1 -Node 5 -Client C
```

Use different client IDs. Clients may connect to any running, ready node.
Followers forward Write and Guess requests to the Leader.

Type these commands **inside the client**, not in PowerShell:

| Command | Meaning |
| --- | --- |
| `write 50` | Set the target to 50 through a quorum commit. |
| `guess 30` | Submit a guess for the current round. The client reads the round first. |
| `read` | Show the connected node's local state. |
| `retry` | Retry the retained request with the same ID and content. |
| `connect 2` | Connect this client to node 2. |
| `help` | Show the available commands. |
| `quit` | Close the client. |

Values must be integers from `0` to `100`.

### Read nodes from PowerShell

Read one node without an interactive client:

```powershell
./scripts/run-client.ps1 -Node 4 -Client inspect -Command 'read'
```

Read all five nodes:

```powershell
1..5 | ForEach-Object { ./scripts/run-client.ps1 -Node $_ -Client inspect -Command 'read' }
```

If you changed the base port, add the same `-BasePort` value to these commands too.

## 5. Understand the state

A client may print a line like this:

```text
node=4 leader=1 role=FOLLOWER ready=true target=50 round=1 last=1:1 guesses=0/2 settling=false
```

| Field | Meaning |
| --- | --- |
| `node` | The node that returned the state. A forwarded operation can return the Leader's state. |
| `leader` | The Leader known by that node, or `unknown`. |
| `role` | `LEADER`, `FOLLOWER`, or `CANDIDATE`. |
| `ready` | Whether the node has completed the required election or synchronization. |
| `target` | The last committed target, or `unset` before the first Write. |
| `round` | The game round. It starts at 0 and increases with each new committed target. |
| `last` | The last committed tag, shown as `epoch:seq`. Before the first commit it is `0:0`. |
| `guesses` | The local guess count and the required count of 2. |
| `settling` | Whether the Leader is committing this round's winner. |

The **round** counts game changes. The **epoch** identifies a Leader generation.
The **sequence** orders proposals within an epoch.
An election changes the epoch, but does not change the game round.
The first new proposal in a new epoch uses sequence 1.

Read returns local committed state. A Follower can briefly show an older value.
Only the Leader keeps the temporary guesses. A Follower's local Read can show `0/2`
while a forwarded Guess response shows the Leader's `1/2`.

## 6. Demonstrate the three required scenarios

Run the scenarios in order. For the exact round numbers below, start with a new data directory.
Keep all five nodes running unless a step asks you to stop one.

### Scenario 1: Normal write

In client A, type:

```text
write 50
read
```

Expected result:

1. Node 4 forwards the Write to the Leader.
2. The Leader saves a `PROPOSED` entry and sends it to Followers.
3. Followers save the proposal to disk before they send an ACK.
4. After at least three durable copies, including its own copy, the Leader commits it.
5. The Leader sends COMMIT to Followers. They apply the new target.

The Leader terminal shows `PROPOSE`, `ACK`, and `COMMIT`.
The client receives `REQUEST_COMMITTED`.
After the nodes apply the commit, they show target `50` and round `1`.
Use the five-node Read command above to check them.

### Scenario 2: Guessing

In client B, type:

```text
guess 30
```

Expected result: `REQUEST_ACCEPTED`, with progress `1/2`.
The target stays `50`, and the round stays `1`.

Then, in client C, type:

```text
guess 62
read
```

Expected result:

- The Leader now has guesses from two different clients.
- The distance from 30 to 50 is 20. The distance from 62 to 50 is 12.
- The Leader selects `62` and replicates it as a `GUESS` proposal.
- After the commit, the target becomes `62`, the round becomes `2`, and the count resets.

The rules are:

- Each round needs exactly two different clients. There is no timer.
- A Write does not count as a guess.
- Each client can submit one guess per round.
- An identical retry does not count twice.
- For equal distances, the first guess received by the Leader wins.
- A committed Write resets the target and expires guesses from the old round.
- Guesses must name the current round. Old-round guesses are rejected.

### Scenario 3: Leader crash and recovery

First, type `read` in a client and note the current `leader` ID.
Keep the Followers running.

To simulate a crash, forcibly stop only the Leader's Java process.
In a separate PowerShell terminal, find the process.
Set `$leaderId` to the ID returned by Read. Set `$basePort` to your current base port.

```powershell
$leaderId = 1
$basePort = 5000
$leaderPid = (Get-NetTCPConnection -LocalPort ($basePort + $leaderId) -State Listen | Select-Object -First 1).OwningProcess
Get-CimInstance Win32_Process -Filter "ProcessId = $leaderPid" | Select-Object ProcessId, Name, CommandLine
```

Check that the command line contains `fr.isep.node.NodeServer` and the correct node ID.
Then stop that process:

```powershell
Stop-Process -Id $leaderPid -Force
```

Do not stop all Java processes. `Ctrl+C` is suitable for normal shutdown.
Use the forced stop above for the crash demonstration.

Expected result:

1. Followers detect the missing heartbeat and start an election.
2. During election and recovery, `ready=false` and the Leader may be `unknown`.
3. Candidates compare their latest logged `(epoch, seq)`. Equal tags favor the lowest node ID.
4. The winner checks the logs and synchronizes the selected history to a majority.
5. It resolves retained pending proposals before printing `LEADER ready`.

The heartbeat interval is configured as 500 ms. The failure timeout is 2500 ms.
Election delays and network work add time. Wait for `ready=true` instead of using a fixed wait.
If the client was connected to the stopped node, type `connect N`, using a running node ID for `N`.

After recovery, the committed target should still be `62` in round `2`.
In a client connected to a running node, type:

```text
read
write 40
read
```

Expected result: the new Leader commits `40` and starts round `3`.
Four running nodes are enough for the fixed quorum of three.

Restart the old Leader in its node terminal. For example, if node 1 stopped:

```powershell
./scripts/run-node.ps1 -Id 1
```

Use the original ID, base port, and data directory.
The restarted node joins as a Follower and catches up with the current Leader.
It does not need another Write to trigger catch-up.
Read all five nodes again. After synchronization, all should show target `40`,
round `3`, and the same last committed tag.

## 7. Request results and retries

| Response | Meaning | Next step |
| --- | --- | --- |
| `REQUEST_ACCEPTED` | The Leader collected a guess. It has not committed a winner yet. | Wait for the other client, then Read. |
| `REQUEST_COMMITTED` | The operation has a durable committed result. | Read other nodes if needed. |
| `REQUEST_REJECTED` | A check failed, or no ready Leader is known. | Read the state and check the value, client, and round. |
| `REQUEST_UNCERTAIN` | The client cannot confirm the final result. | Read and retry the same request if needed. |

An RPC connection error can also leave the result unknown.
Use `retry` in the same client session to keep the original request ID and content.
Do not enter another Write or Guess first, because a new command replaces the retained request.
Changing the value under the same request ID is rejected.
The client does not switch nodes or retry writes automatically.

Guesses that have not become a durable proposal can disappear after a Leader crash.
Read the recovered round before sending a new guess.
With fewer than three available nodes, the cluster cannot commit new operations.

## 8. Local logs and code structure

Each node stores its own persistent journal in `<data-dir>/node-N/entries.log`.
The default data directory is `data`. Data stays on disk when a process stops.
The journal uses UTF-8 text records: `EPOCH`, `ENTRY`, and `SNAPSHOT`.

Each entry includes the PDF's required fields: `epoch`, `seq`, `op`, `value`, and `status`.
It also includes the client ID, request ID, and resulting round for retries and replay.
IDs use URL-safe Base64. Recovery snapshots contain a Base64 Protobuf checkpoint.
`LogStore` calls `FileChannel.force(true)` before a positive persistence ACK.
On restart, it replays the journal. A file lock prevents two processes from writing the same log.

Node messages use **gRPC with binary Protobuf**, defined in `src/main/proto/zookeeper.proto`.
The PDF allows gRPC as a communication choice. Its message section also asks for
line-based or JSON messages. This implementation uses Protobuf on the network.
The local journal uses text records.

| Package under `src/main/java/fr/isep/` | Main responsibility |
| --- | --- |
| `node/` | Configuration, node state, startup, and shutdown. |
| `game/` | Client RPCs, guess validation, and winner selection. |
| `coordination/` | Peer connections, replication, election, and recovery. |
| `storage/` | Journal writes, checkpoints, validation, and replay. |
| `client/` | Client commands, connections, and retained requests. |

`NodeRuntime` connects the components and manages their resources.
`LeaderCoordinator` serializes writes and recovery.
The coordinator checks the epoch and round again before it submits a winning guess.
`RecoveryManager` preserves observed commits and stops recovery if histories conflict.
Retained pending proposals keep their original tags.

## 9. Run the tests

```powershell
./scripts/test.ps1
```

Tests cover storage, game rules, duplicate requests, quorum loss, and recovery.
`ClusterProcessTest` starts five separate JVMs and forcibly stops Leaders.
It also checks crashes at different proposal and commit stages, plus node restart.
Tests use separate temporary data directories and port ranges.
JUnit results are in `target/surefire-reports/`.
Saved test and demo traces are in [docs/evidence](docs/evidence/).

## 10. Run from IntelliJ IDEA

1. Open the project as a Maven project using `pom.xml`.
2. Select JDK 17 or later, then build the project.
3. Create five application run configurations with main class `fr.isep.node.NodeServer`.
4. Set program arguments to `--id 1`, `--id 2`, and so on through `--id 5`.
5. Set the working directory to the project folder for every configuration.
6. Start clients with main class `fr.isep.client.ClientApp` and arguments such as `--node 4 --client A`.

The server main class is `fr.isep.node.NodeServer`.
An old configuration that uses `fr.isep.Node` will fail with `ClassNotFoundException`.

## 11. Common problems

| Problem | What to check |
| --- | --- |
| `java` is not found | Install JDK 17 or later and add its `bin` directory to PATH. |
| Maven is not found | Install Maven or use IntelliJ IDEA's bundled Maven. |
| A script is blocked | Use the process-only execution policy command in section 1. |
| The main class is missing | Run the build. In IntelliJ, check the main class and module classpath. |
| A port is already in use | Stop the old node, or choose another base port for every node and client. |
| The log is locked | Another process may already use that node's data directory. |
| No Leader becomes ready | Start at least three nodes. Check that all nodes use the same base port. |
| The client cannot connect | Check that its chosen node is running. Use `connect N` to select another node. |
| The round differs from the example | Old logs restore old state. Use a new data directory for a fresh demonstration. |

The PowerShell scripts include a workaround for a Windows JDK selector issue.
Keep `target/nonexistent-selector-dir` absent. The scripts pass this path as a JVM option.
They do not need that directory to exist.

## 12. Report and submission

The PDF asks for a report, a source code archive, and this README before the defence.
The report must include screenshots or recordings for all three scenarios,
plus a short explanation of the design and observed results.

- [English report with runtime screenshots](https://docs.google.com/document/d/1Cn01oG_NdFkW3ttg6IEzb0h3mRIHUiw_ldDtNo5AChI/edit).
- [Presentation for the oral defence](https://docs.google.com/presentation/d/1A4wngUTm6wbbFGc35b75SNzPPrC8jkQcnOCvKKFB5d0/edit).
- [Repository test report and saved traces](docs/report.md).

Include `src/`, `scripts/`, `pom.xml`, and `README.md` in the source code archive.
Include the report and its evidence in the submission too.
Generated classes in `target/` and local game data are not needed to rebuild the project.

The teacher may choose any of the three scenarios for each student's live demonstration.
Every group member should know how to run all three. Screenshots do not replace the live demo.

## 13. Scope

This is a simplified lab system for five fixed localhost nodes and process stop/restart.
It demonstrates quorum replication, Leader election, and log recovery.
It does not implement full ZooKeeper, ZAB, or Raft.
It does not guarantee consensus under arbitrary network partitions, arbitrary delays,
Byzantine faults, or changes to cluster membership.
The quorum always stays at three, even after some nodes stop.
