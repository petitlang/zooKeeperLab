package fr.isep.coordination;

import fr.isep.node.NodeConfig;
import fr.isep.node.NodeServer;
import fr.isep.proto.*;
import fr.isep.storage.LogStore;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/** Real localhost gRPC calls with independent per-node durable logs. */
class ReplicationTest {
    @TempDir
    Path directory;
    private int basePort;
    private final Map<Integer, NodeServer> nodes = new HashMap<>();
    private final Map<Integer, ManagedChannel> clients = new HashMap<>();
    private final List<Server> simulatedPeers = new ArrayList<>();

    @BeforeEach
    void choosePorts() throws IOException {
        for (int base = 20000; base < 21000; base += 10) {
            List<ServerSocket> reservations = new ArrayList<>();
            try {
                for (int id = 1; id <= 5; id++) {
                    reservations.add(new ServerSocket(base + id));
                }
                basePort = base;
                return;
            } catch (IOException occupied) {
                // Try another complete five-port range.
            } finally {
                for (ServerSocket reservation : reservations) {
                    reservation.close();
                }
            }
        }
        throw new IOException("No free five-port range for the integration test.");
    }

    private NodeConfig config(int id) {
        return new NodeConfig(id, basePort, directory);
    }

    private void start(int id) throws IOException {
        NodeServer node = new NodeServer(config(id), false);
        try {
            node.start();
            nodes.put(id, node);
            clients.put(id, ManagedChannelBuilder.forAddress("127.0.0.1", basePort + id)
                    .usePlaintext().build());
        } catch (IOException | RuntimeException e) {
            node.close();
            throw e;
        }
    }

    private void startCluster() throws IOException {
        for (int id = 1; id <= 5; id++) {
            start(id);
        }
        nodes.get(1).bootstrapLeader();
    }

    private void stop(int id) throws InterruptedException {
        NodeServer node = nodes.remove(id);
        if (node != null) {
            node.close();
        }
        ManagedChannel client = clients.remove(id);
        if (client != null) {
            client.shutdownNow();
            client.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    private GameServiceGrpc.GameServiceBlockingStub game(int id) {
        return GameServiceGrpc.newBlockingStub(clients.get(id)).withDeadlineAfter(10, TimeUnit.SECONDS);
    }

    private CoordinationServiceGrpc.CoordinationServiceBlockingStub coordination(int id) {
        return CoordinationServiceGrpc.newBlockingStub(clients.get(id)).withDeadlineAfter(3, TimeUnit.SECONDS);
    }

    private static WriteRequest write(String request, int value) {
        return WriteRequest.newBuilder().setClientId("client").setRequestId(request).setValue(value).build();
    }

    private LogSnapshot log(int id) {
        return coordination(id).fetchLog(FetchLogRequest.newBuilder().setFromNodeId(1).build());
    }

    private GameState read(int id) {
        return game(id).read(ReadRequest.getDefaultInstance());
    }

    @AfterEach
    void closeAll() throws InterruptedException {
        for (int id : List.copyOf(nodes.keySet())) {
            stop(id);
        }
        for (Server server : simulatedPeers) {
            server.shutdownNow();
            server.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void writeThroughFollowerCommitsOnAllFiveAndRetriesKeepSameTag() throws Exception {
        startCluster();
        OperationResponse result = game(5).write(write("first", 50));
        assertEquals(RequestStatus.REQUEST_COMMITTED, result.getStatus(), result.getMessage());
        assertEquals(1, result.getCommittedTag().getEpoch());
        assertEquals(1, result.getCommittedTag().getSeq());
        for (int id = 1; id <= 5; id++) {
            assertEquals(50, read(id).getTarget());
            assertEquals(1, read(id).getRoundId());
            assertEquals(LogStatus.COMMITTED, log(id).getEntries(0).getStatus());
        }
        OperationResponse retry = game(3).write(write("first", 50));
        assertEquals(result.getCommittedTag(), retry.getCommittedTag());
        assertEquals(1, log(1).getEntriesCount());
        assertEquals(RequestStatus.REQUEST_REJECTED, game(2).write(write("first", 62)).getStatus());
        assertEquals(50, read(1).getTarget());
    }

    @Test
    void quorumLossLeavesPendingAndSameRequestResumesAfterFollowerReturns() throws Exception {
        startCluster();
        stop(4);
        stop(5);
        assertEquals(RequestStatus.REQUEST_COMMITTED, game(2).write(write("first", 50)).getStatus());
        stop(3);
        long started = System.nanoTime();
        OperationResponse pending = game(2).write(write("second", 62));
        assertEquals(RequestStatus.REQUEST_UNCERTAIN, pending.getStatus());
        assertFalse(pending.hasCommittedTag());
        assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(5), "ACK waiting must be bounded");
        for (int id : List.of(1, 2)) {
            assertEquals(50, read(id).getTarget());
            assertEquals(LogStatus.PROPOSED, log(id).getEntries(1).getStatus());
        }
        assertEquals(RequestStatus.REQUEST_REJECTED, game(1).write(write("third", 70)).getStatus());
        assertEquals(2, log(1).getEntriesCount());
        start(3);
        OperationResponse resumed = game(2).write(write("second", 62));
        assertEquals(RequestStatus.REQUEST_COMMITTED, resumed.getStatus(), resumed.getMessage());
        assertEquals(2, resumed.getCommittedTag().getSeq());
        assertEquals(2, log(1).getEntriesCount());
        start(4);
        start(5);
        OperationResponse next = game(1).write(write("third", 70));
        assertEquals(RequestStatus.REQUEST_COMMITTED, next.getStatus(), next.getMessage());
        for (int id = 1; id <= 5; id++) {
            assertEquals(70, read(id).getTarget());
            assertEquals(3, read(id).getRoundId());
            assertEquals(3, log(id).getEntriesCount());
            assertTrue(log(id).getEntriesList().stream().allMatch(entry -> entry.getStatus() == LogStatus.COMMITTED));
        }
    }

    @Test
    void followerAcknowledgesDurableProposalBeforeCommitAndRestartRestoresBothStages() throws Exception {
        startCluster();
        LogEntry proposal = LogEntry.newBuilder().setTag(ProposalTag.newBuilder().setEpoch(1).setSeq(1))
                .setOp(Operation.WRITE).setValue(0).setStatus(LogStatus.PROPOSED)
                .setClientId("client").setRequestId("rpc-probe").setRoundId(1).build();
        ProposeRequest request = ProposeRequest.newBuilder().setLeaderId(1).setLeaderEpoch(1)
                .setEntry(proposal).build();
        assertTrue(coordination(2).propose(request).getAccepted());
        assertFalse(read(2).hasTarget());
        assertEquals(LogStatus.PROPOSED, log(2).getEntries(0).getStatus());
        stop(2);
        start(2);
        assertEquals(proposal, log(2).getEntries(0));
        assertFalse(read(2).hasTarget());
        assertTrue(coordination(2).announceLeader(LeaderAnnouncement.newBuilder()
                .setLeaderId(1).setLeaderEpoch(1).build()).getAccepted());
        CommitRequest commit = CommitRequest.newBuilder().setLeaderId(1).setLeaderEpoch(1)
                .setTag(proposal.getTag()).build();
        assertTrue(coordination(2).commit(commit).getAccepted());
        assertTrue(coordination(2).commit(commit).getAccepted());
        stop(2);
        start(2);
        assertTrue(read(2).hasTarget());
        assertEquals(0, read(2).getTarget());
        assertEquals(1, read(2).getRoundId());
        assertFalse(read(2).getReady());
    }

    @Test
    void higherPeerEpochFencesLeaderEvenIfOtherPeersAcknowledge() throws Exception {
        startCluster();
        assertTrue(coordination(2).announceLeader(LeaderAnnouncement.newBuilder()
                .setLeaderId(3).setLeaderEpoch(2).build()).getAccepted());
        OperationResponse response = game(1).write(write("fenced", 50));
        assertEquals(RequestStatus.REQUEST_UNCERTAIN, response.getStatus());
        assertFalse(read(1).hasTarget());
        assertEquals(NodeRole.FOLLOWER, read(1).getRole());
        assertEquals(2, log(1).getCurrentEpoch());
        assertEquals(LogStatus.PROPOSED, log(1).getEntries(0).getStatus());
        assertFalse(read(1).getReady());
    }

    @Test
    void mismatchedAckTagsAndNodeIdsCannotCountTowardsQuorum() throws Exception {
        start(1);
        start(4);
        fakePeer(2, true);
        fakePeer(3, false);
        nodes.get(1).bootstrapLeader();
        OperationResponse response = game(1).write(write("bad-acks", 50));
        assertEquals(RequestStatus.REQUEST_UNCERTAIN, response.getStatus());
        assertFalse(read(1).hasTarget());
        assertFalse(read(4).hasTarget());
        assertEquals(LogStatus.PROPOSED, log(1).getEntries(0).getStatus());
    }

    private void fakePeer(int id, boolean wrongNodeId) throws IOException {
        fakePeer(id, wrongNodeId, false);
    }

    private void fakePeer(int id, boolean wrongNodeId, boolean advanceAfterCommit) throws IOException {
        Server server = ServerBuilder.forPort(basePort + id).addService(
                new CoordinationServiceGrpc.CoordinationServiceImplBase() {
                    @Override
                    public void fetchLog(FetchLogRequest request, StreamObserver<LogSnapshot> observer) {
                        observer.onNext(LogSnapshot.newBuilder().setFromNodeId(id).build());
                        observer.onCompleted();
                    }

                    @Override
                    public void announceLeader(LeaderAnnouncement request, StreamObserver<NodeResponse> observer) {
                        observer.onNext(NodeResponse.newBuilder().setFromNodeId(id).setAccepted(true)
                                .setCurrentEpoch(request.getLeaderEpoch()).build());
                        observer.onCompleted();
                    }

                    @Override
                    public void propose(ProposeRequest request, StreamObserver<Ack> observer) {
                        ProposalTag tag = wrongNodeId || advanceAfterCommit ? request.getEntry().getTag()
                                : request.getEntry().getTag().toBuilder().setSeq(999).build();
                        observer.onNext(Ack.newBuilder().setFromNodeId(wrongNodeId && !advanceAfterCommit ? 3 : id)
                                .setTag(tag).setAccepted(true).setCurrentEpoch(request.getLeaderEpoch()).build());
                        observer.onCompleted();
                    }

                    @Override
                    public void commit(CommitRequest request, StreamObserver<NodeResponse> observer) {
                        observer.onNext(NodeResponse.newBuilder().setFromNodeId(id)
                                .setAccepted(!advanceAfterCommit)
                                .setCurrentEpoch(advanceAfterCommit ? request.getLeaderEpoch() + 1
                                        : request.getLeaderEpoch()).build());
                        observer.onCompleted();
                    }
                }).build().start();
        simulatedPeers.add(server);
    }

    @Test
    void higherEpochLearnedAfterDurableCommitCannotTurnSuccessIntoRejection() throws Exception {
        for (int id : List.of(1, 3, 4, 5)) {
            start(id);
        }
        fakePeer(2, false, true);
        nodes.get(1).bootstrapLeader();
        OperationResponse response = game(1).write(write("committed-before-epoch-change", 50));
        assertEquals(RequestStatus.REQUEST_COMMITTED, response.getStatus(), response.getMessage());
        assertTrue(response.hasCommittedTag());
        assertEquals(NodeRole.FOLLOWER, read(1).getRole());
        assertFalse(read(1).getReady());
        assertEquals(2, log(1).getCurrentEpoch());
        assertEquals(LogStatus.COMMITTED, log(1).getEntries(0).getStatus());
        for (int id : List.of(1, 3, 4, 5)) {
            assertEquals(50, read(id).getTarget());
        }
    }

    @Test
    void invalidRequestsDoNotReachTheLogAndUnannouncedSenderCannotCommit() throws Exception {
        startCluster();
        WriteRequest missing = WriteRequest.newBuilder().setClientId("client").setRequestId("missing").build();
        assertEquals(RequestStatus.REQUEST_REJECTED, game(5).write(missing).getStatus());
        assertEquals(RequestStatus.REQUEST_REJECTED, game(5).write(write("invalid", 101)).getStatus());
        assertEquals(0, log(1).getEntriesCount());
        ProposeRequest bad = ProposeRequest.newBuilder().setLeaderId(3).setLeaderEpoch(1)
                .setEntry(LogEntry.newBuilder().setTag(ProposalTag.newBuilder().setEpoch(1).setSeq(1))
                        .setOp(Operation.WRITE).setValue(50).setStatus(LogStatus.PROPOSED).setRoundId(1)).build();
        assertFalse(coordination(2).propose(bad).getAccepted());
        assertFalse(coordination(2).commit(CommitRequest.newBuilder().setLeaderId(1).setLeaderEpoch(1)
                .setTag(ProposalTag.newBuilder().setEpoch(1).setSeq(1)).build()).getAccepted());
        assertEquals(0, log(2).getEntriesCount());
    }

    @Test
    void concurrentClientsCommitOneOrderedHistoryAcrossAllNodes() throws Exception {
        startCluster();
        ExecutorService pool = Executors.newFixedThreadPool(5);
        try {
            List<Future<OperationResponse>> requests = new ArrayList<>();
            for (int id = 1; id <= 5; id++) {
                int clientNode = id;
                requests.add(pool.submit(() -> game(clientNode).write(write("concurrent-" + clientNode, clientNode))));
            }
            for (Future<OperationResponse> request : requests) {
                OperationResponse response = request.get(10, TimeUnit.SECONDS);
                assertEquals(RequestStatus.REQUEST_COMMITTED, response.getStatus(), response.getMessage());
            }
        } finally {
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(3, TimeUnit.SECONDS));
        }
        List<LogEntry> expected = log(1).getEntriesList();
        assertEquals(5, expected.size());
        for (int id = 1; id <= 5; id++) {
            assertEquals(expected, log(id).getEntriesList());
            assertEquals(5, read(id).getRoundId());
            assertEquals(expected.get(4).getValue(), read(id).getTarget());
        }
    }

    @Test
    void bootstrapNeedsMajorityAndRefusesExistingHistory() throws Exception {
        start(1);
        assertThrows(IOException.class, () -> nodes.get(1).bootstrapLeader());
        assertEquals(0, log(1).getCurrentEpoch());
        try (LogStore previous = new LogStore(config(3).getLogFile())) {
            previous.recordEpoch(2);
        }
        for (int id = 2; id <= 5; id++) {
            start(id);
        }
        assertThrows(IOException.class, () -> nodes.get(1).bootstrapLeader());
        assertEquals(NodeRole.FOLLOWER, read(1).getRole());
        assertFalse(read(1).getReady());
    }
    private GuessRequest guess(String client, String request, int value, long round) {
        return GuessRequest.newBuilder().setClientId(client).setRequestId(request).setValue(value).setRoundId(round).build();
    }

    @Test
    void exactlyTwoDistinctClientsSettleAndRetriesDoNotCountTwice() throws Exception {
        startCluster();
        assertEquals(RequestStatus.REQUEST_COMMITTED, game(1).write(write("target", 50)).getStatus());
        GuessRequest first = guess("B", "b1", 30, 1);
        OperationResponse accepted = game(3).guess(first);
        assertEquals(RequestStatus.REQUEST_ACCEPTED, accepted.getStatus());
        assertEquals(1, accepted.getState().getReceivedGuesses());
        assertEquals(2, accepted.getState().getRequiredGuesses());
        assertEquals(RequestStatus.REQUEST_ACCEPTED, game(5).guess(first).getStatus());
        assertEquals(RequestStatus.REQUEST_REJECTED, game(1).guess(guess("B", "b2", 62, 1)).getStatus());
        assertEquals(50, read(1).getTarget());
        GuessRequest second = guess("C", "c1", 62, 1);
        OperationResponse result = game(4).guess(second);
        assertEquals(RequestStatus.REQUEST_COMMITTED, result.getStatus(), result.getMessage());
        assertEquals(62, result.getState().getTarget());
        assertEquals(2, result.getState().getRoundId());
        assertEquals(0, result.getState().getReceivedGuesses());
        assertEquals(RequestStatus.REQUEST_COMMITTED, game(2).guess(second).getStatus());
        assertEquals(RequestStatus.REQUEST_REJECTED, game(2).guess(second.toBuilder().setValue(61).build()).getStatus());
        assertEquals(RequestStatus.REQUEST_REJECTED, game(2).guess(first).getStatus());
        assertEquals(Operation.GUESS, log(1).getEntries(1).getOp());
    }

    @Test
    void tiesUseArrivalOrderAndWriteInvalidatesCollectedGuesses() throws Exception {
        startCluster();
        game(1).write(write("target", 50));
        game(1).guess(guess("B", "b1", 40, 1));
        assertEquals(RequestStatus.REQUEST_COMMITTED, game(1).guess(guess("C", "c1", 60, 1)).getStatus());
        assertEquals(40, read(1).getTarget());
        game(1).guess(guess("B", "b2", 0, 2));
        assertEquals(RequestStatus.REQUEST_COMMITTED, game(1).write(write("reset", 0)).getStatus());
        assertEquals(0, read(1).getReceivedGuesses());
        assertEquals(RequestStatus.REQUEST_REJECTED, game(1).guess(guess("C", "old", 100, 2)).getStatus());
        game(1).guess(guess("B", "b3", 0, 3));
        assertEquals(RequestStatus.REQUEST_COMMITTED, game(1).guess(guess("C", "c3", 100, 3)).getStatus());
        assertEquals(0, read(1).getTarget());
    }

    @Test
    void rejectsInvalidGuessesAndSerializesWriteAgainstSettlement() throws Exception {
        startCluster();
        assertEquals(RequestStatus.REQUEST_REJECTED, game(1).guess(guess("B", "b", 10, 0)).getStatus());
        game(1).write(write("target", 50));
        assertEquals(RequestStatus.REQUEST_REJECTED, game(1).guess(guess("", "bad", 10, 1)).getStatus());
        assertEquals(RequestStatus.REQUEST_REJECTED, game(1).guess(guess("B", "bad", 101, 1)).getStatus());
        assertEquals(RequestStatus.REQUEST_REJECTED, game(1).guess(guess("client", "target", 50, 1)).getStatus());
        game(1).guess(guess("B", "b", 30, 1));
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<OperationResponse> reset = pool.submit(() -> game(2).write(write("reset", 80)));
            Future<OperationResponse> settle = pool.submit(() -> game(3).guess(guess("C", "c", 62, 1)));
            assertEquals(RequestStatus.REQUEST_COMMITTED, reset.get(10, TimeUnit.SECONDS).getStatus());
            RequestStatus status = settle.get(10, TimeUnit.SECONDS).getStatus();
            assertTrue(status == RequestStatus.REQUEST_COMMITTED || status == RequestStatus.REQUEST_REJECTED);
        } finally { pool.shutdownNow(); }
        assertEquals(80, read(1).getTarget());
        assertEquals(0, read(1).getReceivedGuesses());
    }

    @Test
    void pendingWinnerIsRetriedWithoutChoosingAnotherGuess() throws Exception {
        startCluster();
        game(1).write(write("target", 50));
        stop(3); stop(4); stop(5);
        GuessRequest a = guess("B", "b", 30, 1), b = guess("C", "c", 62, 1);
        assertEquals(RequestStatus.REQUEST_ACCEPTED, game(1).guess(a).getStatus());
        assertEquals(RequestStatus.REQUEST_UNCERTAIN, game(1).guess(b).getStatus());
        assertEquals(50, read(1).getTarget());
        assertEquals(62, log(1).getEntries(1).getValue());
        start(3);
        assertEquals(RequestStatus.REQUEST_COMMITTED, game(1).guess(a).getStatus());
        assertEquals(62, read(1).getTarget());
        assertEquals(2, log(1).getEntriesCount());
    }

}
