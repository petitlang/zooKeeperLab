package fr.isep.coordination;

import fr.isep.node.NodeConfig;
import fr.isep.node.NodeRuntime;
import fr.isep.proto.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// 固定五个 localhost 节点，针对 TP 的进程崩溃场景。
public final class ElectionManager {
    private final NodeRuntime node;
    private volatile long lastHeartbeat = System.nanoTime();

    public ElectionManager(NodeRuntime node) {
        this.node = node;
    }

    public void heardLeader() {
        lastHeartbeat = System.nanoTime();
    }

    public void tick() {
        synchronized (node.operationLock) {
            if (!node.running) { return; }
            try {
                GameState state = node.state.snapshot();
                if (state.getRole() == NodeRole.LEADER && state.getReady()) {
                    sendHeartbeats();
                } else if (state.getRole() == NodeRole.LEADER
                        || (System.nanoTime() - lastHeartbeat) / 1_000_000
                        > node.config.getElectionTimeout().toMillis() + node.config.getNodeId() * 150L) {
                    elect();
                }
            } catch (IOException | RuntimeException e) {
                node.state.pause();
                node.print("Coordination paused: " + e.getMessage());
            }
        }
    }

    private void sendHeartbeats() throws IOException {
        int epoch = node.state.getCurrentEpoch();
        int alive = 1;
        HeartbeatRequest heartbeat = HeartbeatRequest.newBuilder()
                .setLeaderId(node.config.getNodeId()).setLeaderEpoch(epoch)
                .setLastCommitted(node.state.snapshot().getLastCommitted()).build();
        for (NodeConfig.NodeAddress peer : node.config.getPeers()) {
            try {
                NodeResponse answer = node.peer(peer.nodeId()).heartbeat(heartbeat);
                if (answer.getCurrentEpoch() > epoch) {
                    node.state.observeHigherEpoch(answer.getCurrentEpoch());
                    return;
                }
                if (!answer.getAccepted()) {
                    answer = sync(peer.nodeId(), node.log.getEntries(), epoch);
                }
                if (answer.getAccepted()) {
                    alive++;
                }
            } catch (RuntimeException e) {
                // 节点可能已被关闭；下次心跳再试。
            }
        }
        if (alive < node.config.getQuorum()) {
            node.state.pause();
            node.print("Heartbeat quorum lost: " + alive + "/5; writes paused.");
        }
    }

    private void elect() throws IOException {
        node.state.beginElection();
        heardLeader(); // 没有 quorum 时也不连续刷选举消息。
        LogSnapshot own = node.logSnapshot();
        List<LogSnapshot> snapshots = new ArrayList<>();
        snapshots.add(own);
        ElectionRequest request = ElectionRequest.newBuilder().setFromNodeId(node.config.getNodeId())
                .setCurrentEpoch(own.getCurrentEpoch()).setLatestTag(latest(own)).build();
        for (NodeConfig.NodeAddress peer : node.config.getPeers()) {
            try {
                node.peer(peer.nodeId()).election(request);
                snapshots.add(node.peer(peer.nodeId()).fetchLog(FetchLogRequest.newBuilder()
                        .setFromNodeId(node.config.getNodeId()).build()));
            } catch (RuntimeException e) {
                // 选举只考虑本次能联系上的节点，但至少要有三个。
            }
        }
        if (snapshots.size() < node.config.getQuorum()) {
            node.print("ELECTION waiting for quorum: " + snapshots.size() + "/5");
            return;
        }
        LogSnapshot winner = own;
        int maxEpoch = own.getCurrentEpoch();
        for (LogSnapshot candidate : snapshots) {
            maxEpoch = Math.max(maxEpoch, candidate.getCurrentEpoch());
            int comparison = compare(latest(candidate), latest(winner));
            if (comparison > 0 || (comparison == 0 && candidate.getFromNodeId() < winner.getFromNodeId())) {
                winner = candidate;
            }
        }
        node.print("ELECTION winner=" + winner.getFromNodeId()
                + " latest=" + LeaderCoordinator.tag(latest(winner)));
        if (winner.getFromNodeId() != node.config.getNodeId()) {
            return; // 获胜节点负责广播，其他节点等它的通知。
        }

        // 把选举和恢复分开：先选出谁，再让它同步日志。
        recover(winner, snapshots, maxEpoch);
    }

    private void recover(LogSnapshot winner, List<LogSnapshot> snapshots, int maxEpoch) throws IOException {
        List<LogEntry> agreed = reconcile(winner, snapshots);
        node.state.observeHigherEpoch(maxEpoch);
        node.state.beginElection();
        node.state.becomeLeader(); // 新 epoch 已持久化，seq 从零开始。
        int epoch = node.state.getCurrentEpoch();
        node.game.clear(); // 尚未进入日志的猜数不保留，客户端需要重交。
        node.state.synchronize(agreed);

        List<Integer> synced = new ArrayList<>();
        synced.add(node.config.getNodeId());
        for (LogSnapshot snapshot : snapshots) {
            int id = snapshot.getFromNodeId();
            if (id == node.config.getNodeId()) {
                continue;
            }
            try {
                NodeResponse announcement = node.peer(id).announceLeader(LeaderAnnouncement.newBuilder()
                        .setLeaderId(node.config.getNodeId()).setLeaderEpoch(epoch).build());
                if (announcement.getAccepted() && sync(id, agreed, epoch).getAccepted()) {
                    synced.add(id);
                }
            } catch (RuntimeException e) {
                // 恢复时也必须等 quorum，不能单节点自行提交。
            }
        }
        if (synced.size() < node.config.getQuorum()) {
            node.print("RECOVERY waiting for quorum.");
            return;
        }
        // 选出的未提交尾部已经重新复制到 quorum，现在按顺序完成提交。
        for (LogEntry entry : agreed) {
            if (entry.getStatus() == LogStatus.PROPOSED) {
                node.state.commit(entry.getTag());
                node.print("RECOVERY COMMIT " + LeaderCoordinator.tag(entry.getTag()));
            }
        }
        int committedCopies = 1;
        for (int id : synced) {
            if (id != node.config.getNodeId()) {
                try {
                    if (sync(id, node.log.getEntries(), epoch).getAccepted()) {
                        committedCopies++;
                    }
                } catch (RuntimeException e) {
                    // 下次选举仍保留已经提交的记录。
                }
            }
        }
        synchronized (node.state) {
            if (committedCopies >= node.config.getQuorum()
                    && node.state.getCurrentEpoch() == epoch
                    && node.state.snapshot().getRole() == NodeRole.LEADER) {
                node.state.markReady();
                node.print("LEADER ready epoch=" + epoch + " seq=" + node.state.getCurrentSeq()
                        + " synchronized=" + committedCopies + "/5");
            }
        }
    }

    private NodeResponse sync(int id, List<LogEntry> entries, int epoch) {
        return node.peer(id).syncLog(SyncLogRequest.newBuilder()
                .setLeaderId(node.config.getNodeId()).setLeaderEpoch(epoch).addAllEntries(entries).build());
    }

    // 在本 TP 的崩溃模型中，各日志应为同一日志的前缀；发现冲突就停止恢复。
    static List<LogEntry> reconcile(LogSnapshot winner, List<LogSnapshot> snapshots) {
        List<LogEntry> agreed = new ArrayList<>(winner.getEntriesList());
        for (LogSnapshot snapshot : snapshots) {
            if (snapshot.getEntriesCount() > agreed.size()) {
                throw new IllegalStateException("Winner does not contain the longest log.");
            }
            for (int i = 0; i < snapshot.getEntriesCount(); i++) {
                LogEntry other = snapshot.getEntries(i);
                LogEntry selected = agreed.get(i);
                if (!other.toBuilder().setStatus(LogStatus.PROPOSED).build()
                        .equals(selected.toBuilder().setStatus(LogStatus.PROPOSED).build())) {
                    throw new IllegalStateException("Conflicting logs; recovery stopped.");
                }
                if (other.getStatus() == LogStatus.COMMITTED) {
                    agreed.set(i, selected.toBuilder().setStatus(LogStatus.COMMITTED).build());
                }
            }
        }
        return agreed;
    }

    private static ProposalTag latest(LogSnapshot snapshot) {
        return snapshot.getEntriesCount() == 0 ? ProposalTag.getDefaultInstance()
                : snapshot.getEntries(snapshot.getEntriesCount() - 1).getTag();
    }

    public static int compare(ProposalTag a, ProposalTag b) {
        int epoch = Integer.compare(a.getEpoch(), b.getEpoch());
        return epoch != 0 ? epoch : Integer.compare(a.getSeq(), b.getSeq());
    }
}
