# ZooKeeper-like lab

Java 17 and gRPC implementation of the Lab3 coordination system.

## Current stage

Five independent localhost nodes can start and register both `GameService` and
`CoordinationService` on their respective ports. The RPC handlers, election,
replication, persistent logging, and game rules will be implemented in subsequent
steps. Calling an existing RPC currently returns `UNIMPLEMENTED`.

## Compile

Run commands from `D:\ZooKeeper\ZooKeeper` with JDK 17 or later and Maven:

```powershell
Set-Location 'D:\ZooKeeper\ZooKeeper'
mvn compile
```

## Start five nodes

Open five terminals in the project directory. Run one command in each terminal:

```powershell
mvn exec:java "-Dexec.mainClass=fr.isep.node.NodeServer" "-Dexec.args=--id 1"
mvn exec:java "-Dexec.mainClass=fr.isep.node.NodeServer" "-Dexec.args=--id 2"
mvn exec:java "-Dexec.mainClass=fr.isep.node.NodeServer" "-Dexec.args=--id 3"
mvn exec:java "-Dexec.mainClass=fr.isep.node.NodeServer" "-Dexec.args=--id 4"
mvn exec:java "-Dexec.mainClass=fr.isep.node.NodeServer" "-Dexec.args=--id 5"
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
Data directories are created on startup; log files are not written at this stage.

Use Ctrl+C to stop a node. Orderly JVM shutdown runs the server shutdown hook;
forced process termination may bypass it. Leader-crash recovery requires the
election and replication implementation planned for later steps.

## Startup options

- `--id`: required node ID from 1 to 5.
- `--base-port`: default 5000. Listening port is base port + node ID. All nodes
  must use the same base port; the valid range is 0 through 65530.
- `--data-dir`: default `data`, resolved relative to the working directory.
  Each node gets a `node-<id>` subdirectory.
- `--help`: print usage and exit.

Example using ports 6001 through 6005 and another data root:

```powershell
mvn exec:java "-Dexec.mainClass=fr.isep.node.NodeServer" "-Dexec.args=--id 1 --base-port 6000 --data-dir local-data"
```

For IntelliJ IDEA, run `fr.isep.node.NodeServer` with program arguments `--id 1`
and the project directory as the working directory. Create five run
configurations, changing the ID to 1 through 5.

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
