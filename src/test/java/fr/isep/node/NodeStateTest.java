package fr.isep.node;

import fr.isep.proto.LogEntry;
import fr.isep.proto.NodeRole;
import fr.isep.proto.Operation;
import fr.isep.storage.LogStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class NodeStateTest {
    @TempDir
    Path directory;

    private NodeConfig config(int id) {
        return new NodeConfig(id, 5000, directory);
    }

    private static void elect(NodeState state) throws Exception {
        state.beginElection();
        state.becomeLeader();
        state.markReady();
    }

    @Test
    void targetChangesOnlyOnCommitAndZeroIsARealTarget() throws Exception {
        try (LogStore log = new LogStore(config(1).getLogFile())) {
            NodeState state = new NodeState(config(1), log);
            assertFalse(state.snapshot().hasTarget());
            elect(state);
            LogEntry first = state.createProposal(Operation.WRITE, 0, "a", "1");
            assertFalse(state.snapshot().hasTarget());
            state.commit(first.getTag());
            assertTrue(state.snapshot().hasTarget());
            assertEquals(0, state.snapshot().getTarget());
            LogEntry second = state.createProposal(Operation.GUESS, 62, "b", "2");
            assertEquals(0, state.snapshot().getTarget());
            state.commit(second.getTag());
            state.commit(first.getTag()); // An old duplicate cannot roll back the target.
            assertEquals(62, state.snapshot().getTarget());
            assertEquals(2, state.snapshot().getRoundId());
        }
    }

    @Test
    void restartRestoresDataButNotLeaderAuthorityOrUncommittedTarget() throws Exception {
        try (LogStore log = new LogStore(config(1).getLogFile())) {
            NodeState state = new NodeState(config(1), log);
            elect(state);
            LogEntry committed = state.createProposal(Operation.WRITE, 50, "a", "1");
            state.commit(committed.getTag());
            state.createProposal(Operation.GUESS, 62, "b", "2");
        }
        try (LogStore restored = new LogStore(config(1).getLogFile())) {
            NodeState state = new NodeState(config(1), restored);
            assertEquals(50, state.snapshot().getTarget());
            assertEquals(1, state.snapshot().getRoundId());
            assertEquals(2, state.getCurrentSeq());
            assertEquals(2, state.getLatestTag().orElseThrow().getSeq());
            assertEquals(NodeRole.FOLLOWER, state.snapshot().getRole());
            assertFalse(state.snapshot().hasLeaderId());
            assertFalse(state.snapshot().getReady());
            assertThrows(IllegalStateException.class,
                    () -> state.createProposal(Operation.WRITE, 20, "c", "3"));
            state.beginElection();
            state.becomeLeader();
            assertThrows(IllegalStateException.class, state::markReady);
        }
    }

    @Test
    void epochWithoutProposalsSurvivesRestartAndNewLeaderResetsSequence() throws Exception {
        try (LogStore log = new LogStore(config(1).getLogFile())) {
            NodeState state = new NodeState(config(1), log);
            elect(state);
            LogEntry first = state.createProposal(Operation.WRITE, 50, "a", "1");
            state.commit(first.getTag());
            state.beginElection();
            state.becomeLeader(); // Epoch 2 has no entry yet.
            assertEquals(0, state.getCurrentSeq());
        }
        try (LogStore log = new LogStore(config(1).getLogFile())) {
            NodeState state = new NodeState(config(1), log);
            assertEquals(2, state.getCurrentEpoch());
            assertEquals(0, state.getCurrentSeq());
            assertEquals(1, state.getLatestTag().orElseThrow().getEpoch());
            elect(state);
            LogEntry next = state.createProposal(Operation.GUESS, 62, "b", "2");
            assertEquals(3, next.getTag().getEpoch());
            assertEquals(1, next.getTag().getSeq());
            assertEquals(2, next.getRoundId());
        }
    }

    @Test
    void followerRecordsProposalBeforeApplyingCommitAndRejectsOldEpoch() throws Exception {
        try (LogStore leaderLog = new LogStore(config(1).getLogFile());
             LogStore followerLog = new LogStore(config(2).getLogFile())) {
            NodeState leader = new NodeState(config(1), leaderLog);
            NodeState follower = new NodeState(config(2), followerLog);
            elect(leader);
            follower.followLeader(1, leader.getCurrentEpoch());
            follower.markReady();
            LogEntry proposal = leader.createProposal(Operation.WRITE, 50, "a", "1");
            follower.receiveProposal(proposal);
            assertFalse(follower.snapshot().hasTarget());
            assertEquals(proposal, followerLog.getLatestEntry().orElseThrow());
            follower.commit(proposal.getTag());
            assertEquals(50, follower.snapshot().getTarget());
            assertThrows(IllegalArgumentException.class, () -> follower.followLeader(3, 1));
            follower.observeHigherEpoch(2);
            assertFalse(follower.snapshot().getReady());
            assertFalse(follower.snapshot().hasLeaderId());
            assertThrows(IllegalArgumentException.class, () -> follower.receiveProposal(proposal));
            assertThrows(IllegalArgumentException.class, () -> follower.followLeader(1, 1));
        }
    }

    @Test
    void concurrentRequestsGetUniqueDurableOrderedTagsAndReplayConsistently() throws Exception {
        int count = 32;
        try (LogStore log = new LogStore(config(1).getLogFile())) {
            NodeState state = new NodeState(config(1), log);
            elect(state);
            ExecutorService pool = Executors.newFixedThreadPool(8);
            List<LogEntry> proposals = new ArrayList<>();
            try {
                List<Future<LogEntry>> requests = new ArrayList<>();
                for (int i = 0; i < count; i++) {
                    int value = i;
                    requests.add(pool.submit(() -> state.createProposal(
                            Operation.WRITE, value, "client-" + value, "request-" + value)));
                }
                for (Future<LogEntry> request : requests) {
                    proposals.add(request.get(10, TimeUnit.SECONDS));
                }
            } finally {
                pool.shutdownNow();
                assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
            }
            assertEquals(count, proposals.stream().map(LogEntry::getTag).distinct().count());
            assertFalse(state.snapshot().hasTarget());
            proposals.sort(Comparator.comparingInt(entry -> entry.getTag().getSeq()));
            for (int i = 0; i < count; i++) {
                assertEquals(i + 1, proposals.get(i).getTag().getSeq());
                state.commit(proposals.get(i).getTag());
            }
            assertEquals(count, state.snapshot().getRoundId());
            assertEquals(proposals.get(count - 1).getValue(), state.snapshot().getTarget());
        }
        try (LogStore restored = new LogStore(config(1).getLogFile())) {
            NodeState state = new NodeState(config(1), restored);
            assertEquals(count, state.getCurrentSeq());
            assertEquals(count, state.snapshot().getRoundId());
        }
    }

    @Test
    void invalidProposalDoesNotConsumeSequenceAndFollowerCannotPropose() throws Exception {
        try (LogStore log = new LogStore(config(1).getLogFile())) {
            NodeState state = new NodeState(config(1), log);
            assertThrows(IllegalStateException.class, state::markReady);
            assertThrows(IllegalStateException.class, state::becomeLeader);
            elect(state);
            assertThrows(IllegalArgumentException.class,
                    () -> state.createProposal(Operation.WRITE, 101, "a", "1"));
            assertEquals(0, state.getCurrentSeq());
            assertEquals(1, state.createProposal(Operation.WRITE, 50, "a", "2").getTag().getSeq());
            state.observeHigherEpoch(2);
            assertThrows(IllegalStateException.class,
                    () -> state.createProposal(Operation.WRITE, 40, "a", "3"));
        }
    }
}
