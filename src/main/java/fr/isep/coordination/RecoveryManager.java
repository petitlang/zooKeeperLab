package fr.isep.coordination;

import fr.isep.node.*;
import fr.isep.proto.*;
import fr.isep.storage.LogStore;
import java.io.IOException;
import java.util.*;

/** Reconciles a compatible history and makes it durable on a fixed majority. */
public final class RecoveryManager {
    private final NodeConfig config;
    private final NodeState state;
    private final PeerTransport transport;
    private final LeaderCoordinator coordinator;

    public RecoveryManager(NodeConfig config, NodeState state, PeerTransport transport, LeaderCoordinator coordinator) {
        this.config = config;
        this.state = state;
        this.transport = transport;
        this.coordinator = coordinator;
    }

    static int compareCandidates(LogSnapshot left, LogSnapshot right) {
        ProposalTag a = latest(left), b = latest(right);
        int order = Integer.compare(a.getEpoch(), b.getEpoch());
        if (order == 0) order = Integer.compare(a.getSeq(), b.getSeq());
        if (order == 0) order = Integer.compare(right.getFromNodeId(), left.getFromNodeId());
        return order;
    }

    private static ProposalTag latest(LogSnapshot snapshot) {
        return snapshot.getEntriesCount() == 0 ? ProposalTag.getDefaultInstance()
                : snapshot.getEntries(snapshot.getEntriesCount() - 1).getTag();
    }

    /** Pure reconciliation; conflicts fail closed, even if only a proposal conflicts. */
    static List<LogEntry> reconcile(LogSnapshot winner, List<LogSnapshot> snapshots) {
        List<LogEntry> result = new ArrayList<>(winner.getEntriesList());
        int committedThrough = -1;
        for (LogSnapshot snapshot : snapshots) {
            LogStore.validateSnapshot(snapshot.getEntriesList(), snapshot.getCurrentEpoch());
            if (snapshot.getEntriesCount() > result.size()) throw new IllegalArgumentException("Winner is missing history.");
            for (int i = 0; i < snapshot.getEntriesCount(); i++) {
                LogEntry a = result.get(i), b = snapshot.getEntries(i);
                if (!a.toBuilder().setStatus(LogStatus.PROPOSED).build()
                        .equals(b.toBuilder().setStatus(LogStatus.PROPOSED).build())) {
                    throw new IllegalArgumentException("Contradictory history at " + i);
                }
                if (b.getStatus() == LogStatus.COMMITTED) committedThrough = Math.max(committedThrough, i);
            }
        }
        for (int i = 0; i <= committedThrough; i++) result.set(i, result.get(i).toBuilder()
                .setStatus(LogStatus.COMMITTED).build());
        return List.copyOf(result);
    }

    /** Shares the coordinator's operation monitor, never the state monitor across RPCs. */
    boolean electAndRecover() throws IOException {
        synchronized (coordinator) {
            if (Thread.currentThread().isInterrupted()) return false;
            state.beginElection();
            List<LogSnapshot> snapshots = new ArrayList<>();
            snapshots.add(state.logSnapshot());
            for (PeerTransport.Reply<LogSnapshot> reply : transport.callAll((peer, stub) -> {
                ElectionRequest.Builder candidate = ElectionRequest.newBuilder().setFromNodeId(config.getNodeId())
                        .setCurrentEpoch(state.getCurrentEpoch());
                state.getLatestTag().ifPresent(candidate::setLatestTag);
                ElectionResponse response = stub.election(candidate.build());
                if (response.getFromNodeId() != peer.nodeId()) throw new IllegalArgumentException("Wrong candidate ID.");
                return stub.fetchLog(FetchLogRequest.newBuilder().setFromNodeId(config.getNodeId()).build());
            })) {
                if (reply.error() == null && reply.value().getFromNodeId() == reply.nodeId()) snapshots.add(reply.value());
            }
            if (snapshots.size() < config.getQuorum()) return false;
            LogSnapshot winner = snapshots.stream().max(RecoveryManager::compareCandidates).orElseThrow();
            if (winner.getFromNodeId() != config.getNodeId()) return false;
            List<LogEntry> history = reconcile(winner, snapshots);
            int epoch = Math.addExact(Math.max(state.getCurrentEpoch(), snapshots.stream()
                    .mapToInt(LogSnapshot::getCurrentEpoch).max().orElse(0)), 1);
            synchronized (state) {
                if (state.snapshot().getRole() != NodeRole.CANDIDATE || Thread.currentThread().isInterrupted()) return false;
                state.becomeLeader(epoch);
                state.installRecoveredLog(config.getNodeId(), epoch, history);
            }
            if (!syncMajority(epoch, history)) return false;
            System.out.println("[Node " + config.getNodeId() + "] RECOVERY synchronized entries=" + history.size()
                    + " quorum=3 epoch=" + epoch);
            // Each pending entry has now been durably copied to a majority under this authority.
            for (LogEntry entry : history) {
                if (entry.getStatus() == LogStatus.PROPOSED) {
                    synchronized (state) {
                        if (!leader(epoch)) return false;
                        state.commit(entry.getTag());
                        System.out.println("[Node " + config.getNodeId() + "] RECOVERY COMMIT "
                                + entry.getTag().getEpoch() + ":" + entry.getTag().getSeq());
                    }
                    if (!syncMajority(epoch, state.logSnapshot().getEntriesList())) return false;
                }
            }
            synchronized (state) {
                if (!leader(epoch) || Thread.currentThread().isInterrupted()) return false;
                state.markReady();
            }
            System.out.println("[Node " + config.getNodeId() + "] LEADER ready epoch=" + epoch);
            return true;
        }
    }

    private boolean syncMajority(int epoch, List<LogEntry> history) throws IOException {
        int accepted = 1;
        for (PeerTransport.Reply<NodeResponse> reply : transport.callAll((peer, stub) -> {
            NodeResponse announcement = stub.announceLeader(LeaderAnnouncement.newBuilder()
                    .setLeaderId(config.getNodeId()).setLeaderEpoch(epoch).build());
            if (!announcement.getAccepted()) return announcement;
            return stub.syncLog(SyncLogRequest.newBuilder().setLeaderId(config.getNodeId())
                    .setLeaderEpoch(epoch).addAllEntries(history).build());
        })) {
            if (reply.error() == null && reply.value().getFromNodeId() == reply.nodeId()) {
                state.observeHigherEpoch(reply.value().getCurrentEpoch());
                if (reply.value().getAccepted() && reply.value().getCurrentEpoch() == epoch) accepted++;
            }
        }
        return accepted >= config.getQuorum() && leader(epoch) && !Thread.currentThread().isInterrupted();
    }

    /** Repair unavailable/restarted followers without waiting for another client write. */
    void catchUpFollowers() throws IOException {
        synchronized (coordinator) {
            int epoch = state.getCurrentEpoch();
            if (!leader(epoch) || !state.snapshot().getReady()) return;
            List<LogEntry> history = state.logSnapshot().getEntriesList();
            for (PeerTransport.Reply<NodeResponse> reply : transport.callAll((peer, stub) -> {
                LogSnapshot remote = stub.fetchLog(FetchLogRequest.newBuilder().setFromNodeId(config.getNodeId()).build());
                if (remote.getFromNodeId() != peer.nodeId()) throw new IllegalArgumentException("Wrong peer.");
                if (remote.getCurrentEpoch() > epoch) return NodeResponse.newBuilder().setFromNodeId(peer.nodeId())
                        .setCurrentEpoch(remote.getCurrentEpoch()).build();
                if (remote.getCurrentEpoch() == epoch && remote.getReady() && remote.getEntriesList().equals(history))
                    return NodeResponse.newBuilder().setFromNodeId(peer.nodeId()).setAccepted(true)
                            .setCurrentEpoch(epoch).build();
                NodeResponse announcement = stub.announceLeader(LeaderAnnouncement.newBuilder()
                        .setLeaderId(config.getNodeId()).setLeaderEpoch(epoch).build());
                if (!announcement.getAccepted()) return announcement;
                // Sync also establishes readiness of a restarted follower with an identical log.
                return stub.syncLog(SyncLogRequest.newBuilder().setLeaderId(config.getNodeId())
                        .setLeaderEpoch(epoch).addAllEntries(history).build());
            })) {
                if (reply.error() == null && reply.value().getFromNodeId() == reply.nodeId())
                    state.observeHigherEpoch(reply.value().getCurrentEpoch());
            }
        }
    }

    private boolean leader(int epoch) {
        GameState snapshot = state.snapshot();
        return snapshot.getRole() == NodeRole.LEADER && snapshot.hasLeaderId()
                && snapshot.getLeaderId() == config.getNodeId() && state.getCurrentEpoch() == epoch;
    }
}
