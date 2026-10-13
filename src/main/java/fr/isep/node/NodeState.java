package fr.isep.node;

import fr.isep.proto.GameState;
import fr.isep.proto.LogEntry;
import fr.isep.proto.LogStatus;
import fr.isep.proto.LogSnapshot;
import fr.isep.proto.NodeRole;
import fr.isep.proto.Operation;
import fr.isep.proto.ProposalTag;
import fr.isep.storage.LogStore;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/** Shared runtime state: role, leader, epoch, and committed game state. */
public final class NodeState {
    private final int nodeId;
    private final LogStore logStore;
    private NodeRole role = NodeRole.FOLLOWER;
    private Integer leaderId;
    private int currentEpoch;
    private int currentSeq;
    private Integer target;
    private long roundId;
    private ProposalTag lastCommitted;
    private boolean ready;

    public NodeState(NodeConfig config, LogStore logStore) {
        this.nodeId = Objects.requireNonNull(config, "config").getNodeId();
        this.logStore = Objects.requireNonNull(logStore, "logStore");
        currentEpoch = logStore.getCurrentEpoch();
        currentSeq = logStore.getLatestEntry()
                .filter(entry -> entry.getTag().getEpoch() == currentEpoch)
                .map(entry -> entry.getTag().getSeq()).orElse(0);
        restoreCommittedState();
        // A restart restores data, but cannot assume the previous leadership is valid.
    }

    public synchronized void beginElection() {
        role = NodeRole.CANDIDATE;
        leaderId = null;
        ready = false;
    }

    /** Call only after winning the lab's election and reconciling the logs. */
    public synchronized void becomeLeader() throws IOException {
        if (role != NodeRole.CANDIDATE) {
            throw new IllegalStateException("Only an election candidate can become leader.");
        }
        int newEpoch = Math.addExact(currentEpoch, 1);
        logStore.recordEpoch(newEpoch);
        currentEpoch = newEpoch;
        currentSeq = 0;
        role = NodeRole.LEADER;
        leaderId = nodeId;
        ready = false;
    }

    public synchronized void followLeader(int id, int epoch) throws IOException {
        if (id < 1 || id > NodeConfig.NODE_COUNT || id == nodeId) {
            throw new IllegalArgumentException("Expected another cluster node as leader.");
        }
        if (epoch <= 0 || epoch < currentEpoch) {
            throw new IllegalArgumentException("Cannot follow an old or invalid epoch.");
        }
        if (epoch == currentEpoch && leaderId != null && leaderId != id) {
            throw new IllegalArgumentException("Conflicting leader in the same epoch.");
        }
        boolean changed = epoch != currentEpoch || leaderId == null || leaderId != id
                || role != NodeRole.FOLLOWER;
        logStore.recordEpoch(epoch);
        currentEpoch = epoch;
        currentSeq = logStore.getLatestEntry()
                .filter(entry -> entry.getTag().getEpoch() == currentEpoch)
                .map(entry -> entry.getTag().getSeq()).orElse(0);
        role = NodeRole.FOLLOWER;
        leaderId = id;
        if (changed) {
            ready = false;
        }
    }

    /** A higher epoch fences off the previous leader; the new leader is not known yet. */
    public synchronized void observeHigherEpoch(int epoch) throws IOException {
        if (epoch <= currentEpoch) {
            return;
        }
        logStore.recordEpoch(epoch);
        currentEpoch = epoch;
        currentSeq = 0;
        role = NodeRole.FOLLOWER;
        leaderId = null;
        ready = false;
    }

    /** The coordinator must verify synchronization/quorum before calling this. */
    public synchronized void markReady() {
        if (leaderId == null || role == NodeRole.CANDIDATE) {
            throw new IllegalStateException("No established leader.");
        }
        if (logStore.getEntries().stream().anyMatch(entry -> entry.getStatus() == LogStatus.PROPOSED)) {
            throw new IllegalStateException("Resolve pending proposals before resuming operation.");
        }
        ready = true;
    }

    /** Allocate and durably log a proposal under one lock; it is still invisible. */
    public synchronized LogEntry createProposal(Operation op, int value,
                                                String clientId, String requestId) throws IOException {
        if (role != NodeRole.LEADER || !ready) {
            throw new IllegalStateException("Only a ready leader can create proposals.");
        }
        int nextSeq = Math.addExact(currentSeq, 1);
        long nextRound = Math.addExact(logStore.getLatestEntry()
                .map(LogEntry::getRoundId).orElse(0L), 1);
        LogEntry proposal = LogEntry.newBuilder()
                .setTag(ProposalTag.newBuilder().setEpoch(currentEpoch).setSeq(nextSeq))
                .setOp(Objects.requireNonNull(op, "op")).setValue(value)
                .setStatus(LogStatus.PROPOSED)
                .setClientId(Objects.requireNonNull(clientId, "clientId"))
                .setRequestId(Objects.requireNonNull(requestId, "requestId"))
                .setRoundId(nextRound).build();
        logStore.appendProposal(proposal);
        currentSeq = nextSeq;
        return proposal;
    }

    /** The RPC handler must first validate the sender's leader ID and epoch. */
    public synchronized LogEntry receiveProposal(LogEntry proposal) throws IOException {
        if (!proposal.hasTag() || proposal.getTag().getEpoch() != currentEpoch) {
            throw new IllegalArgumentException("Proposal must match the current epoch.");
        }
        LogEntry recorded = logStore.appendProposal(proposal);
        currentSeq = Math.max(currentSeq, proposal.getTag().getSeq());
        return recorded;
    }

    /** Publish target/round only after the COMMITTED record is durable. */
    public synchronized LogEntry commit(ProposalTag tag) throws IOException {
        LogEntry committed = logStore.commit(tag);
        restoreCommittedState();
        return committed;
    }

    private void restoreCommittedState() {
        Optional<LogEntry> latest = logStore.getLatestCommittedEntry();
        target = latest.map(LogEntry::getValue).orElse(null);
        roundId = latest.map(LogEntry::getRoundId).orElse(0L);
        lastCommitted = latest.map(LogEntry::getTag).orElse(null);
    }

    /** Immutable transport snapshot; callers cannot modify the live state. */
    public synchronized GameState snapshot() {
        GameState.Builder result = GameState.newBuilder()
                .setNodeId(nodeId).setRole(role).setRoundId(roundId).setReady(ready);
        if (target != null) {
            result.setTarget(target);
        }
        if (leaderId != null) {
            result.setLeaderId(leaderId);
        }
        if (lastCommitted != null) {
            result.setLastCommitted(lastCommitted);
        }
        return result.build();
    }

    public synchronized int getCurrentEpoch() {
        return currentEpoch;
    }

    public synchronized int getCurrentSeq() {
        return currentSeq;
    }

    /** Includes PROPOSED entries, as required for the lab election comparison. */
    public synchronized Optional<ProposalTag> getLatestTag() {
        return logStore.getLatestEntry().map(LogEntry::getTag);
    }

    public synchronized LogSnapshot logSnapshot() {
        return LogSnapshot.newBuilder().setFromNodeId(nodeId).setCurrentEpoch(currentEpoch)
                .addAllEntries(logStore.getEntries()).build();
    }

    public synchronized void pause() {
        ready = false;
    }
}
