package fr.isep;

import fr.isep.coordination.*;
import fr.isep.game.*;
import fr.isep.node.*;
import fr.isep.proto.*;
import fr.isep.storage.LogStore;
import io.grpc.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.io.*;
import java.net.ServerSocket;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import static org.junit.jupiter.api.Assertions.*;

/** Five-process integration tests, including crashes during replication. */
class ClusterProcessTest {
    @TempDir Path data;
    private final Map<Integer, Process> processes = new HashMap<>();
    private final Map<Integer, ManagedChannel> channels = new HashMap<>();
    private Path evidence;
    private int base;

    @BeforeEach void prepare() throws Exception {
        Path root = Path.of("target", "demo-evidence");
        Files.createDirectories(root);
        evidence = Files.createTempDirectory(root, "run-").toAbsolutePath();
        for (int candidate = 23000; candidate < 24000; candidate += 10) {
            List<ServerSocket> reserved = new ArrayList<>();
            try {
                for (int id = 1; id <= 5; id++) reserved.add(new ServerSocket(candidate + id));
                base = candidate;
                break;
            } catch (IOException ignored) {
            } finally { for (ServerSocket socket : reserved) socket.close(); }
        }
        assertTrue(base > 0);
    }

    private Process launch(int id, String main, String... extra) throws IOException {
        List<String> command = new ArrayList<>(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Djdk.net.unixdomain.tmpdir=" + data.resolve("nonexistent-selector-dir"),
                "-cp", System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                main, "--id", Integer.toString(id), "--base-port", Integer.toString(base), "--data-dir", data.toString()));
        command.addAll(List.of(extra));
        return new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(evidence.resolve("node-" + id + "-" + System.nanoTime() + ".log").toFile()).start();
    }

    private void start(int id) throws IOException {
        processes.put(id, launch(id, NodeServer.class.getName()));
        channels.computeIfAbsent(id, ignored -> ManagedChannelBuilder.forAddress("127.0.0.1", base + id).usePlaintext().build());
    }

    private void kill(int id) throws Exception {
        Process process = processes.remove(id);
        if (process != null) { process.destroyForcibly(); assertTrue(process.waitFor(5, TimeUnit.SECONDS)); }
    }

    private GameServiceGrpc.GameServiceBlockingStub game(int id) {
        return GameServiceGrpc.newBlockingStub(channels.get(id)).withDeadlineAfter(6, TimeUnit.SECONDS);
    }

    private GameState read(int id) { return game(id).read(ReadRequest.getDefaultInstance()); }

    private void await(BooleanSupplier condition, String reason) throws Exception {
        long end = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        Throwable last = null;
        while (System.nanoTime() < end) {
            try { if (condition.getAsBoolean()) return; }
            catch (RuntimeException error) { last = error; }
            Thread.sleep(100);
        }
        fail(reason + "; evidence=" + evidence + "; last=" + last);
    }

    private int leader() {
        int leader = -1;
        for (int id : processes.keySet()) {
            GameState state = read(id);
            if (state.getRole() == NodeRole.LEADER && state.getReady()) {
                if (leader != -1) return -1;
                leader = id;
            }
        }
        return leader;
    }

    private WriteRequest write(String request, int value) {
        return WriteRequest.newBuilder().setClientId("A").setRequestId(request).setValue(value).build();
    }

    private GuessRequest guess(String client, int value, long round) {
        return GuessRequest.newBuilder().setClientId(client).setRequestId(client + "-" + round)
                .setValue(value).setRoundId(round).build();
    }

    private void record(String text) throws IOException {
        Files.writeString(evidence.resolve("scenarios.txt"), text + System.lineSeparator(),
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    @AfterEach void cleanup() throws Exception {
        for (int id : List.copyOf(processes.keySet())) kill(id);
        for (ManagedChannel channel : channels.values()) channel.shutdownNow();
        for (ManagedChannel channel : channels.values()) channel.awaitTermination(2, TimeUnit.SECONDS);
    }

    @Test void writeGuessCrashRejoinAndResumeAcrossFiveJvms() throws Exception {
        for (int id = 1; id <= 5; id++) start(id);
        await(() -> leader() > 0 && processes.keySet().stream().allMatch(id -> read(id).getReady()), "Automatic election");
        int oldLeader = leader();
        int follower = processes.keySet().stream().filter(id -> id != oldLeader).findFirst().orElseThrow();
        OperationResponse first = game(follower).write(write("initial", 50));
        assertEquals(RequestStatus.REQUEST_COMMITTED, first.getStatus(), first.getMessage());
        record("WRITE target=50 tag=" + first.getCommittedTag());
        assertEquals(RequestStatus.REQUEST_ACCEPTED, game(follower).guess(guess("B", 30, 1)).getStatus());
        OperationResponse winner = game(follower).guess(guess("C", 62, 1));
        assertEquals(RequestStatus.REQUEST_COMMITTED, winner.getStatus(), winner.getMessage());
        await(() -> processes.keySet().stream().allMatch(id -> read(id).getTarget() == 62), "Guess replication");
        record("GUESS 30,62 -> target=62 round=2");
        List<String> cli = List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Djdk.net.unixdomain.tmpdir=" + data.resolve("nonexistent-selector-dir"),
                "-cp", System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                "fr.isep.client.ClientApp", "--node", Integer.toString(follower), "--base-port", Integer.toString(base),
                "--client", "inspect", "--command", "read");
        Path cliOutput = evidence.resolve("client-read.txt");
        Process client = new ProcessBuilder(cli).redirectErrorStream(true).redirectOutput(cliOutput.toFile()).start();
        try {
            assertTrue(client.waitFor(10, TimeUnit.SECONDS));
            assertEquals(0, client.exitValue());
            assertTrue(Files.readString(cliOutput).contains("target=62"));
        } finally { client.destroyForcibly(); }
        kill(oldLeader);
        await(() -> leader() > 0 && leader() != oldLeader, "Leader failover");
        int elected = leader();
        await(() -> processes.keySet().stream().allMatch(id -> read(id).getReady()
                && read(id).getLeaderId() == elected), "Followers recognize recovered leader");
        assertEquals(62, read(elected).getTarget());
        assertEquals(RequestStatus.REQUEST_COMMITTED, game(elected).write(write("after-crash", 40)).getStatus());
        record("CRASH old=" + oldLeader + " new=" + elected + " resumed target=40");
        start(oldLeader);
        await(() -> read(oldLeader).getReady() && read(oldLeader).getTarget() == 40, "Restart catches up without another write");
        long round = read(elected).getRoundId();
        game(oldLeader).guess(guess("B", 39, round));
        assertEquals(RequestStatus.REQUEST_COMMITTED, game(oldLeader).guess(guess("C", 80, round)).getStatus());
        assertEquals(39, read(elected).getTarget());
        record("REJOIN original node caught up; GUESS resumed target=39");
        List<Integer> removed = processes.keySet().stream().filter(id -> id != elected).limit(3).toList();
        for (int id : removed) kill(id);
        WriteRequest pending = write("quorum-missing", 100);
        assertEquals(RequestStatus.REQUEST_UNCERTAIN, game(elected).write(pending).getStatus());
        assertEquals(39, read(elected).getTarget());
        start(removed.get(0));
        await(() -> game(elected).write(pending).getStatus() == RequestStatus.REQUEST_COMMITTED,
                "Same request succeeds when majority returns");
        assertEquals(100, read(elected).getTarget());
        record("QUORUM 2/5 held target=39; same request after 3/5 committed target=100");
    }

    @ParameterizedTest @ValueSource(ints = {1, 2, 3})
    void crashAtProtocolBoundariesPreservesDecisions(int stage) throws Exception {
        processes.put(1, launch(1, StagedLeader.class.getName(), "--stage", Integer.toString(stage)));
        channels.put(1, ManagedChannelBuilder.forAddress("127.0.0.1", base + 1).usePlaintext().build());
        for (int id = 2; id <= 5; id++) start(id);
        await(() -> Files.exists(data.resolve("stage-ready")), "Fixture reaches crash boundary " + stage);
        kill(1);
        await(() -> leader() > 0 && processes.keySet().stream().allMatch(id -> read(id).getReady()), "Boundary recovery " + stage);
        int elected = leader();
        int expected = stage == 1 ? 50 : 62;
        await(() -> processes.keySet().stream().allMatch(id -> read(id).getTarget() == expected), "Recovered target");
        assertEquals(RequestStatus.REQUEST_COMMITTED, game(elected).write(write("boundary-resume", 70)).getStatus());
        start(1);
        await(() -> read(1).getReady() && read(1).getTarget() == 70, "Old leader rejoins with original log");
        record("BOUNDARY stage=" + stage + " recovered=" + expected + " newLeader=" + elected + " resumed=70");
    }

    /** Leader fixture that pauses at each replication stage. */
    public static final class StagedLeader {
        public static void main(String[] args) throws Exception {
            int stage = Integer.parseInt(args[args.length - 1]);
            NodeConfig config = NodeConfig.fromArgs(Arrays.copyOf(args, args.length - 2));
            try (LogStore store = new LogStore(config.getLogFile()); PeerTransport transport = new PeerTransport(config)) {
                NodeState state = new NodeState(config, store);
                LeaderCoordinator coordinator = new LeaderCoordinator(config, state, transport);
                RecoveryManager recovery = new RecoveryManager(config, state, transport, coordinator);
                try (ElectionManager election = new ElectionManager(config, state, transport, recovery)) {
                    GameEngine game = new GameEngine(config, state, coordinator, transport);
                    Server server = ServerBuilder.forPort(config.getPort()).addService(new GameServiceImpl(game))
                            .addService(new CoordinationServiceImpl(state, game, election)).build().start();
                    try {
                        while (transport.callAll((peer, stub) -> stub.fetchLog(FetchLogRequest.newBuilder()
                                .setFromNodeId(1).build())).stream().filter(r -> r.error() == null).count() < 4) Thread.sleep(50);
                        coordinator.bootstrapLeader();
                        OperationResponse first = coordinator.submitWrite(WriteRequest.newBuilder().setClientId("A")
                                .setRequestId("initial").setValue(50).build());
                        if (first.getStatus() != RequestStatus.REQUEST_COMMITTED) throw new IllegalStateException(first.getMessage());
                        LogEntry proposal = state.createProposal(Operation.WRITE, 62, "A", "boundary");
                        if (stage >= 2) {
                            long accepted = transport.callAll((peer, stub) -> {
                                if (peer.nodeId() > 3) return false;
                                return stub.propose(ProposeRequest.newBuilder().setLeaderId(1).setLeaderEpoch(1)
                                        .setEntry(proposal).build()).getAccepted();
                            }).stream().filter(r -> r.error() == null && Boolean.TRUE.equals(r.value())).count();
                            if (accepted != 2) throw new IllegalStateException("Did not persist majority");
                        }
                        if (stage == 3) {
                            state.commit(proposal.getTag());
                            boolean delivered = transport.callAll((peer, stub) -> peer.nodeId() == 2 && stub.commit(CommitRequest.newBuilder()
                                    .setLeaderId(1).setLeaderEpoch(1).setTag(proposal.getTag()).build()).getAccepted())
                                    .stream().anyMatch(reply -> reply.nodeId() == 2 && reply.error() == null
                                            && Boolean.TRUE.equals(reply.value()));
                            if (!delivered) throw new IllegalStateException("Partial commit was not delivered");
                        }
                        Files.writeString(config.getDataDirectory().getParent().resolve("stage-ready"), "stage=" + stage);
                        server.awaitTermination();
                    } finally { server.shutdownNow(); }
                }
            }
        }
    }
}
