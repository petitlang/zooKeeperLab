package fr.isep;

import fr.isep.client.ClientApp;
import fr.isep.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import static org.junit.jupiter.api.Assertions.*;

/** Real independent JVMs: no mock network and no shared node state. */
class ClusterTest {
    @TempDir Path data;
    private final Process[] nodes = new Process[6];
    private final ManagedChannel[] channels = new ManagedChannel[6];
    private final int basePort = Integer.getInteger("lab.test.basePort", 18500);
    private final Path evidence = Path.of("target", "demo-evidence");

    @Test
    void writesGuessesCrashRecoveryPendingTailAndQuorum() throws Exception {
        Files.createDirectories(evidence);
        for (int id = 1; id <= 5; id++) {
            Files.writeString(evidence.resolve("node-" + id + ".log"), "");
        }
        Files.writeString(evidence.resolve("scenarios.txt"), "Five independent JVMs, actual gRPC results\n");
        try {
            for (int id = 1; id <= 5; id++) {
                channels[id] = ManagedChannelBuilder.forAddress("127.0.0.1", basePort + id).usePlaintext().build();
                start(id);
            }
            await(() -> allReady(List.of(1, 2, 3, 4, 5)), 25);
            assertEquals(1, read(1).getLeaderId(), "Equal empty tags must choose lowest ID");
            record("INITIAL ELECTION", List.of(1, 2, 3, 4, 5));

            OperationResponse written = game(4).write(write("A", "write-50", 50));
            assertEquals(RequestStatus.REQUEST_COMMITTED, written.getStatus());
            await(() -> allTarget(List.of(1, 2, 3, 4, 5), 50, 1), 8);
            record("1. WRITE 50 via follower 4", List.of(1, 2, 3, 4, 5));
            Path clientOutput = evidence.resolve("client-read.txt");
            Process client = new ProcessBuilder(
                    Path.of(System.getProperty("java.home"), "bin", "java.exe").toString(),
                    "-Djdk.net.unixdomain.tmpdir=" + data.resolve("nonexistent-socket-dir"), "-cp",
                    System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                    "fr.isep.client.ClientApp", "--node", "4", "--base-port", Integer.toString(basePort),
                    "--client", "cli-test", "read")
                    .redirectErrorStream(true).redirectOutput(clientOutput.toFile()).start();
            try {
                assertTrue(client.waitFor(10, TimeUnit.SECONDS));
                assertEquals(0, client.exitValue(), Files.readString(clientOutput));
                assertTrue(Files.readString(clientOutput).contains("target=50"));
            } finally {
                if (client.isAlive()) {
                    client.destroyForcibly().waitFor(5, TimeUnit.SECONDS);
                }
            }
            OperationResponse duplicate = game(5).write(write("A", "write-50", 50));
            assertEquals(written.getCommittedTag(), duplicate.getCommittedTag());
            assertEquals(RequestStatus.REQUEST_REJECTED,
                    game(2).write(write("bad", "bad-101", 101)).getStatus());

            OperationResponse guess30 = game(3).guess(guess("B", "guess-30", 30, 1));
            OperationResponse guess62 = game(5).guess(guess("C", "guess-62", 62, 1));
            assertEquals(RequestStatus.REQUEST_ACCEPTED, guess30.getStatus());
            assertEquals(RequestStatus.REQUEST_ACCEPTED, guess62.getStatus());
            assertEquals(50, read(1).getTarget(), "Queued guesses must not update target");
            await(() -> allTarget(List.of(1, 2, 3, 4, 5), 62, 2), 10);
            record("2. GUESS 30 and 62: closest to 50 is 62", List.of(1, 2, 3, 4, 5));
            assertEquals(RequestStatus.REQUEST_REJECTED,
                    game(2).guess(guess("D", "old-round", 61, 1)).getStatus());

            int leader = read(3).getLeaderId();
            stop(leader);
            await(() -> allReady(List.of(2, 3, 4, 5)) && read(3).getLeaderId() != leader, 25);
            assertEquals(2, read(3).getLeaderId());
            record("3. KILL leader 1: leader 2 elected, target preserved", List.of(2, 3, 4, 5));
            OperationResponse afterCrash = game(5).write(write("D", "write-40", 40));
            assertEquals(RequestStatus.REQUEST_COMMITTED, afterCrash.getStatus());
            assertTrue(afterCrash.getCommittedTag().getEpoch() > written.getCommittedTag().getEpoch());
            assertEquals(1, afterCrash.getCommittedTag().getSeq(), "First write in new epoch starts at seq 1");
            await(() -> allTarget(List.of(2, 3, 4, 5), 40, 3), 8);
            record("WRITE 40 after crash (new epoch, seq=1)", List.of(2, 3, 4, 5));
            start(1);
            await(() -> allReady(List.of(1, 2, 3, 4, 5)) && allTarget(List.of(1, 2, 3, 4, 5), 40, 3), 15);
            record("RESTART node 1: catches up as follower", List.of(1, 2, 3, 4, 5));

            // Controlled injection: reproduce a crash after PROPOSE but before COMMIT.
            int epoch = coordination(2).fetchLog(FetchLogRequest.getDefaultInstance()).getCurrentEpoch();
            ProposalTag pendingTag = ProposalTag.newBuilder().setEpoch(epoch).setSeq(2).build();
            LogEntry pending = LogEntry.newBuilder().setTag(pendingTag).setOp(Operation.WRITE)
                    .setValue(77).setRoundId(4).setClientId("injection").setRequestId("pending-77")
                    .setStatus(LogStatus.PROPOSED).build();
            stop(2);
            for (int id : List.of(3, 4)) {
                assertTrue(coordination(id).propose(ProposeRequest.newBuilder().setLeaderId(2)
                        .setLeaderEpoch(epoch).setEntry(pending).build()).getAccepted());
                assertEquals(40, read(id).getTarget());
            }
            record("FAULT INJECTION: pending 77 on nodes 3/4; target remains 40", List.of(1, 3, 4, 5));
            await(() -> allReady(List.of(1, 3, 4, 5)) && allTarget(List.of(1, 3, 4, 5), 77, 4), 25);
            assertEquals(3, read(4).getLeaderId(), "Latest tag must beat lower ID with an older log");
            record("RECOVER pending tail: latest-tag node 3 wins and commits 77", List.of(1, 3, 4, 5));

            stop(1);
            stop(5);
            OperationResponse noQuorum = game(3).write(write("E", "no-quorum", 18));
            assertNotEquals(RequestStatus.REQUEST_COMMITTED, noQuorum.getStatus());
            assertEquals(77, read(3).getTarget());
            assertEquals(77, read(4).getTarget());
            record("ONLY 2/5 alive: WRITE 18 is not committed", List.of(3, 4));
            start(5);
            await(() -> allReady(List.of(3, 4, 5)), 25);
            // Uncertain writes may be committed by recovery once a quorum returns.
            int expected = noQuorum.getStatus() == RequestStatus.REQUEST_UNCERTAIN ? 18 : 77;
            assertTrue(allTarget(List.of(3, 4, 5), expected, expected == 18 ? 5 : 4));
            record("QUORUM restored: recovered state target=" + expected, List.of(3, 4, 5));
            assertEquals(RequestStatus.REQUEST_COMMITTED,
                    game(4).write(write("F", "write-zero", 0)).getStatus());
            await(() -> allTarget(List.of(3, 4, 5), 0, expected == 18 ? 6 : 5), 8);
            record("Boundary value 0 is a valid target", List.of(3, 4, 5));
            Files.writeString(evidence.resolve("scenarios.txt"), "\nALL ASSERTIONS PASSED\n", StandardOpenOption.APPEND);
        } finally {
            for (int id = 1; id <= 5; id++) {
                stop(id);
                if (channels[id] != null) {
                    channels[id].shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
                }
            }
        }
    }

    private void start(int id) throws Exception {
        ProcessBuilder builder = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java.exe").toString(),
                "-Djdk.net.unixdomain.tmpdir=" + data.resolve("nonexistent-socket-dir"), "-cp",
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                "fr.isep.node.NodeServer", "--id", Integer.toString(id), "--base-port",
                Integer.toString(basePort), "--data-dir", data.toString());
        builder.redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.appendTo(
                evidence.resolve("node-" + id + ".log").toFile()));
        nodes[id] = builder.start();
    }

    private void stop(int id) throws Exception {
        if (nodes[id] != null && nodes[id].isAlive()) {
            nodes[id].destroyForcibly();
            assertTrue(nodes[id].waitFor(5, TimeUnit.SECONDS));
        }
    }

    private GameServiceGrpc.GameServiceBlockingStub game(int id) {
        return GameServiceGrpc.newBlockingStub(channels[id]).withDeadlineAfter(6, TimeUnit.SECONDS);
    }

    private CoordinationServiceGrpc.CoordinationServiceBlockingStub coordination(int id) {
        return CoordinationServiceGrpc.newBlockingStub(channels[id]).withDeadlineAfter(2, TimeUnit.SECONDS);
    }

    private GameState read(int id) {
        return game(id).withDeadlineAfter(300, TimeUnit.MILLISECONDS).read(ReadRequest.getDefaultInstance());
    }

    private boolean allReady(List<Integer> ids) {
        int leader = read(ids.get(0)).getLeaderId();
        for (int id : ids) {
            GameState state = read(id);
            if (!state.getReady() || !state.hasLeaderId() || state.getLeaderId() != leader) {
                return false;
            }
        }
        return true;
    }

    private boolean allTarget(List<Integer> ids, int target, long round) {
        for (int id : ids) {
            GameState state = read(id);
            if (!state.hasTarget() || state.getTarget() != target || state.getRoundId() != round) {
                return false;
            }
        }
        return true;
    }

    private void record(String title, List<Integer> ids) throws Exception {
        List<String> lines = new ArrayList<>();
        lines.add("\n" + title);
        for (int id : ids) {
            lines.add(ClientApp.format(read(id)));
        }
        Files.write(evidence.resolve("scenarios.txt"), lines, StandardOpenOption.APPEND);
    }

    private static void await(BooleanSupplier condition, int seconds) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (System.nanoTime() < deadline) {
            try {
                if (condition.getAsBoolean()) {
                    return;
                }
            } catch (RuntimeException e) {
                // Servers may still be starting or temporarily unreachable.
            }
            Thread.sleep(150);
        }
        fail("Timed out waiting for cluster; see target/demo-evidence/node-*.log");
    }

    private static WriteRequest write(String client, String request, int value) {
        return WriteRequest.newBuilder().setClientId(client).setRequestId(request).setValue(value).build();
    }

    private static GuessRequest guess(String client, String request, int value, long round) {
        return GuessRequest.newBuilder().setClientId(client).setRequestId(request).setValue(value).setRoundId(round).build();
    }
}
