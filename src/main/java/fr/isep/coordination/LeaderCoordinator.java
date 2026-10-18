package fr.isep.coordination;

import fr.isep.node.NodeConfig;
import fr.isep.node.NodeState;
import fr.isep.proto.*;
import io.grpc.StatusRuntimeException;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

/** Proposal tags, replication, quorum acknowledgments, and commit delivery. */
public final class LeaderCoordinator implements AutoCloseable {
    private final NodeConfig config;
    private final NodeState state;
    private final PeerTransport transport;
    private volatile boolean closed;

    public LeaderCoordinator(NodeConfig config, NodeState state, PeerTransport transport) {
        this.config = Objects.requireNonNull(config, "config");
        this.state = Objects.requireNonNull(state, "state");
        this.transport = Objects.requireNonNull(transport, "transport");
    }

    /** Temporary development bootstrap for a fresh cluster, not the lab election algorithm. */
    public synchronized void bootstrapLeader() throws IOException {
        LogSnapshot local = state.logSnapshot();
        if (closed || local.getCurrentEpoch() != 0 || local.getEntriesCount() != 0) {
            throw new IOException("Manual bootstrap requires a fresh local log and epoch zero.");
        }
        List<PeerResult> discovered = callPeers((peer, stub) -> {
            LogSnapshot snapshot = stub.fetchLog(FetchLogRequest.newBuilder()
                    .setFromNodeId(config.getNodeId()).build());
            if (snapshot.getFromNodeId() != peer.nodeId()) {
                return new PeerResult(peer.nodeId(), false, 0, "Wrong responding node ID.");
            }
            return new PeerResult(peer.nodeId(), snapshot.getCurrentEpoch() == 0
                    && snapshot.getEntriesCount() == 0, snapshot.getCurrentEpoch(), "Discovery",
                    snapshot.getEntriesCount() != 0);
        });
        if (discovered.stream().anyMatch(peer -> peer.hasHistory() || peer.epoch() != 0)) {
            throw new IOException("Existing cluster history needs election/recovery, not manual bootstrap.");
        }
        if (1 + acceptedCount(discovered) < config.getQuorum() || Thread.currentThread().isInterrupted()) {
            throw new IOException("Start at least two fresh followers before bootstrapping the leader.");
        }
        state.beginElection();
        state.becomeLeader();
        int epoch = state.getCurrentEpoch();
        List<PeerResult> announcements = callPeers((peer, stub) -> response(peer, stub.announceLeader(
                LeaderAnnouncement.newBuilder().setLeaderId(config.getNodeId()).setLeaderEpoch(epoch).build()), epoch));
        observeEpochs(announcements);
        if (!isLeaderOfEpoch(epoch) || 1 + acceptedCount(announcements) < config.getQuorum()
                || Thread.currentThread().isInterrupted()) {
            state.pause();
            throw new IOException("Leader announcement did not reach a majority.");
        }
        callPeers((peer, stub) -> response(peer, stub.syncLog(SyncLogRequest.newBuilder()
                .setLeaderId(config.getNodeId()).setLeaderEpoch(epoch).build()), epoch));
        state.markReady();
        System.out.println("[Node " + config.getNodeId() + "] MANUAL LEADER bootstrap, epoch=" + epoch);
    }

    /** Any node can receive a client Write; followers forward without logging it. */
    public OperationResponse submitWrite(WriteRequest request) {
        if (!request.hasValue()) {
            return result(RequestStatus.REQUEST_REJECTED, request.getRequestId(), "Missing value.", null);
        }
        GameState snapshot = state.snapshot();
        if (snapshot.getRole() == NodeRole.LEADER) {
            return commitValue(Operation.WRITE, request.getValue(), request.getClientId(), request.getRequestId());
        }
        if (closed || !snapshot.hasLeaderId() || !snapshot.getReady()) {
            return result(RequestStatus.REQUEST_REJECTED, request.getRequestId(), "No ready leader is known.", null);
        }
        try {
            return transport.forward(snapshot.getLeaderId(), ForwardRequest.newBuilder()
                    .setFromNodeId(config.getNodeId()).setWrite(request).build());
        } catch (StatusRuntimeException e) {
            return result(RequestStatus.REQUEST_UNCERTAIN, request.getRequestId(),
                    "Leader RPC failed; retry the same request ID: " + e.getStatus().getCode(), null);
        }
    }

    public synchronized OperationResponse commitGuess(GuessRequest request, int expectedEpoch) {
        LogEntry existing = state.findRequest(request.getClientId(), request.getRequestId()).orElse(null);
        if (existing == null && (state.getCurrentEpoch() != expectedEpoch
                || state.snapshot().getRoundId() != request.getRoundId())) {
            return result(RequestStatus.REQUEST_REJECTED, request.getRequestId(), "Round or leader changed.", null);
        }
        return commitValue(Operation.GUESS, request.getValue(), request.getClientId(), request.getRequestId());
    }

    /** Serialize writes so later operations cannot commit ahead of an unresolved proposal. */
    private synchronized OperationResponse commitValue(Operation op, int value, String clientId, String requestId) {
        if (closed || value < 0 || value > 100 || clientId.isBlank() || requestId.isBlank()
                || (op != Operation.WRITE && op != Operation.GUESS)) {
            return result(RequestStatus.REQUEST_REJECTED, requestId, "Invalid operation, value, or request identity.", null);
        }
        int epoch = state.getCurrentEpoch();
        if (!ownsEpoch(epoch)) {
            return result(RequestStatus.REQUEST_REJECTED, requestId, "This node is not the ready leader.", null);
        }
        boolean proposalLogged = false;
        ProposalTag decidedTag = null;
        try {
            List<LogEntry> history = state.logSnapshot().getEntriesList();
            LogEntry proposal = history.stream().filter(entry -> entry.getClientId().equals(clientId)
                    && entry.getRequestId().equals(requestId)).findFirst().orElse(null);
            if (proposal != null) {
                if (proposal.getOp() != op || proposal.getValue() != value) {
                    return result(RequestStatus.REQUEST_REJECTED, requestId,
                            "Request ID was already used with different data.", null);
                }
                if (proposal.getStatus() == LogStatus.COMMITTED) {
                    return result(RequestStatus.REQUEST_COMMITTED, requestId, "Already committed.", proposal.getTag());
                }
                if (proposal.getTag().getEpoch() != epoch) {
                    return result(RequestStatus.REQUEST_UNCERTAIN, requestId, "Pending request needs recovery.", null);
                }
            } else {
                if (history.stream().anyMatch(entry -> entry.getStatus() == LogStatus.PROPOSED)) {
                    return result(RequestStatus.REQUEST_REJECTED, requestId,
                            "Retry or recover the unresolved request before submitting another write.", null);
                }
                synchronized (state) {
                    if (!ownsEpoch(epoch)) {
                        return result(RequestStatus.REQUEST_REJECTED, requestId, "Leadership changed.", null);
                    }
                    proposal = state.createProposal(op, value, clientId, requestId);
                }
                history = state.logSnapshot().getEntriesList();
            }
            proposalLogged = true;
            LogEntry current = proposal;
            List<LogEntry> prefix = history;
            System.out.println("[Node " + config.getNodeId() + "] PROPOSE " + tagText(current.getTag())
                    + " value=" + value + " local durable ACK=1");
            List<PeerResult> acknowledgments = callPeers((peer, stub) -> replicatePrefix(peer, stub, prefix, epoch));
            observeEpochs(acknowledgments);
            Set<Integer> confirmed = new HashSet<>();
            confirmed.add(config.getNodeId());
            acknowledgments.stream().filter(PeerResult::accepted).forEach(peer -> confirmed.add(peer.nodeId()));
            System.out.println("[Node " + config.getNodeId() + "] ACK nodes=" + confirmed
                    + " quorum=" + config.getQuorum());
            if (!ownsEpoch(epoch)) {
                return result(RequestStatus.REQUEST_UNCERTAIN, requestId,
                        "Leadership changed before commit; election/recovery is required.", null);
            }
            if (confirmed.size() < config.getQuorum() || Thread.currentThread().isInterrupted()) {
                return result(RequestStatus.REQUEST_UNCERTAIN, requestId,
                        "Not committed: confirmed " + confirmed.size() + "/5, need 3; retry the same request ID.", null);
            }
            synchronized (state) {
                if (!ownsEpoch(epoch)) {
                    return result(RequestStatus.REQUEST_UNCERTAIN, requestId, "Leadership changed before commit.", null);
                }
                state.commit(current.getTag());
                decidedTag = current.getTag();
            }
            System.out.println("[Node " + config.getNodeId() + "] COMMIT " + tagText(current.getTag()));
            List<PeerResult> commits = callPeers((peer, stub) -> response(peer, stub.commit(
                    commitRequest(current.getTag(), epoch)), epoch));
            observeEpochs(commits);
            return result(RequestStatus.REQUEST_COMMITTED, requestId,
                    "Committed; applied on " + (1 + acceptedCount(commits))
                            + "/5 nodes. Lagging nodes catch up on a later write.", current.getTag());
        } catch (IOException e) {
            state.pause();
            if (decidedTag != null) {
                return result(RequestStatus.REQUEST_COMMITTED, requestId,
                        "Committed before a later persistence/broadcast error; recovery is required.", decidedTag);
            }
            return result(RequestStatus.REQUEST_UNCERTAIN, requestId, "Persistent log operation failed.", null);
        } catch (IllegalArgumentException | IllegalStateException | ArithmeticException e) {
            if (decidedTag != null) {
                return result(RequestStatus.REQUEST_COMMITTED, requestId,
                        "Committed before a later broadcast error.", decidedTag);
            }
            if (proposalLogged) {
                state.pause();
                return result(RequestStatus.REQUEST_UNCERTAIN, requestId,
                        "Proposal is logged but its outcome requires recovery: " + e.getMessage(), null);
            }
            return result(RequestStatus.REQUEST_REJECTED, requestId, e.getMessage(), null);
        }
    }

    /** Replay the known committed prefix to late/missed followers before the current proposal. */
    private PeerResult replicatePrefix(NodeConfig.NodeAddress peer,
                                      CoordinationServiceGrpc.CoordinationServiceBlockingStub stub,
                                      List<LogEntry> history, int epoch) {
        PeerResult announcement = response(peer, stub.announceLeader(LeaderAnnouncement.newBuilder()
                .setLeaderId(config.getNodeId()).setLeaderEpoch(epoch).build()), epoch);
        if (!announcement.accepted()) {
            return announcement;
        }
        List<LogEntry> proposals = history;
        if (history.stream().anyMatch(entry -> entry.getTag().getEpoch() != epoch)) {
            // Recover old tags using a checkpoint, then use normal PROPOSE/ACK for the new write.
            PeerResult synchronizedPrefix = response(peer, stub.syncLog(SyncLogRequest.newBuilder()
                    .setLeaderId(config.getNodeId()).setLeaderEpoch(epoch)
                    .addAllEntries(history.subList(0, history.size() - 1)).build()), epoch);
            if (!synchronizedPrefix.accepted()) return synchronizedPrefix;
            proposals = List.of(history.get(history.size() - 1));
        }
        for (LogEntry entry : proposals) {
            Ack ack = stub.propose(ProposeRequest.newBuilder().setLeaderId(config.getNodeId()).setLeaderEpoch(epoch)
                    .setEntry(entry.toBuilder().setStatus(LogStatus.PROPOSED)).build());
            if (ack.getFromNodeId() != peer.nodeId()) {
                return new PeerResult(peer.nodeId(), false, 0, "Wrong ACK node ID.");
            }
            if (!ack.getAccepted() || ack.getCurrentEpoch() != epoch || !ack.hasTag()
                    || !ack.getTag().equals(entry.getTag())) {
                return new PeerResult(peer.nodeId(), false, ack.getCurrentEpoch(), "Rejected or mismatched ACK.");
            }
            if (entry.getStatus() == LogStatus.COMMITTED) {
                PeerResult committed = response(peer, stub.commit(commitRequest(entry.getTag(), epoch)), epoch);
                if (!committed.accepted()) {
                    return committed;
                }
            }
        }
        return new PeerResult(peer.nodeId(), true, epoch, "Current proposal acknowledged.");
    }

    private CommitRequest commitRequest(ProposalTag tag, int epoch) {
        return CommitRequest.newBuilder().setLeaderId(config.getNodeId()).setLeaderEpoch(epoch).setTag(tag).build();
    }

    private PeerResult response(NodeConfig.NodeAddress peer, NodeResponse response, int epoch) {
        if (response.getFromNodeId() != peer.nodeId()) {
            return new PeerResult(peer.nodeId(), false, 0, "Wrong responding node ID.");
        }
        return new PeerResult(peer.nodeId(), response.getAccepted() && response.getCurrentEpoch() == epoch,
                response.getCurrentEpoch(), response.getMessage());
    }

    /** One shared deadline per phase; four peers are contacted concurrently. */
    private List<PeerResult> callPeers(BiFunction<NodeConfig.NodeAddress,
            CoordinationServiceGrpc.CoordinationServiceBlockingStub, PeerResult> call) {
        return transport.callAll(call).stream().filter(reply -> reply.error() == null)
                .map(PeerTransport.Reply::value).toList();
    }

    private void observeEpochs(List<PeerResult> peers) throws IOException {
        int highest = peers.stream().mapToInt(PeerResult::epoch).max().orElse(0);
        state.observeHigherEpoch(highest);
    }

    private boolean ownsEpoch(int epoch) {
        return state.snapshot().getReady() && isLeaderOfEpoch(epoch);
    }

    private boolean isLeaderOfEpoch(int epoch) {
        GameState snapshot = state.snapshot();
        return !closed && snapshot.getRole() == NodeRole.LEADER
                && snapshot.hasLeaderId() && snapshot.getLeaderId() == config.getNodeId()
                && state.getCurrentEpoch() == epoch;
    }

    private static int acceptedCount(List<PeerResult> results) {
        return (int) results.stream().filter(PeerResult::accepted).map(PeerResult::nodeId).distinct().count();
    }

    private OperationResponse result(RequestStatus status, String requestId, String message, ProposalTag tag) {
        OperationResponse.Builder response = OperationResponse.newBuilder().setStatus(status)
                .setRequestId(requestId).setMessage(message).setState(state.snapshot());
        if (tag != null) {
            response.setCommittedTag(tag);
        }
        return response.build();
    }

    private static String tagText(ProposalTag tag) {
        return "(" + tag.getEpoch() + "," + tag.getSeq() + ")";
    }

    private record PeerResult(int nodeId, boolean accepted, int epoch, String message, boolean hasHistory) {
        private PeerResult(int nodeId, boolean accepted, int epoch, String message) {
            this(nodeId, accepted, epoch, message, false);
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
    }
}
