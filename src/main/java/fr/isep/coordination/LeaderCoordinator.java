package fr.isep.coordination;

import fr.isep.node.NodeConfig;
import fr.isep.node.NodeRuntime;
import fr.isep.proto.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// Leader 负责编号、复制、统计 ACK，最后提交。
public final class LeaderCoordinator {
    private final NodeRuntime node;

    public LeaderCoordinator(NodeRuntime node) {
        this.node = node;
    }

    public OperationResponse write(Operation op, int value, String client, String request) {
        synchronized (node.operationLock) {
            try {
                GameState state = node.state.snapshot();
                if (state.getRole() != NodeRole.LEADER || !state.getReady()) {
                    return node.reply(RequestStatus.REQUEST_REJECTED, "Leader is not ready.", request);
                }
                // 同一个请求重试时，不再写入第二条记录。
                for (LogEntry old : node.log.getEntries()) {
                    if (old.getClientId().equals(client) && old.getRequestId().equals(request)) {
                        if (old.getValue() != value || old.getOp() != op) {
                            return node.reply(RequestStatus.REQUEST_REJECTED, "Request ID already used.", request);
                        }
                        return old.getStatus() == LogStatus.COMMITTED ? committed(old)
                                : node.reply(RequestStatus.REQUEST_UNCERTAIN, "Proposal still pending recovery.", request);
                    }
                }
                LogEntry entry = node.state.createProposal(op, value, client, request);
                int epoch = entry.getTag().getEpoch();
                node.print("PROPOSE " + tag(entry.getTag()) + " " + op + " value=" + value);
                List<Integer> acknowledgments = new ArrayList<>();
                acknowledgments.add(node.config.getNodeId()); // Leader 自己也有一份持久化日志。
                for (NodeConfig.NodeAddress peer : node.config.getPeers()) {
                    try {
                        Ack ack = node.peer(peer.nodeId()).propose(ProposeRequest.newBuilder()
                                .setLeaderId(node.config.getNodeId()).setLeaderEpoch(epoch).setEntry(entry).build());
                        if (ack.getCurrentEpoch() > epoch) {
                            node.state.observeHigherEpoch(ack.getCurrentEpoch());
                        }
                        if (ack.getAccepted() && ack.getFromNodeId() == peer.nodeId()
                                && ack.getCurrentEpoch() == epoch && ack.getTag().equals(entry.getTag())) {
                            acknowledgments.add(peer.nodeId());
                            node.print("ACK " + tag(entry.getTag()) + " from=" + peer.nodeId());
                        }
                    } catch (RuntimeException e) {
                        node.print("No ACK from=" + peer.nodeId());
                    }
                }
                synchronized (node.state) {
                    if (acknowledgments.size() < node.config.getQuorum()
                            || node.state.snapshot().getRole() != NodeRole.LEADER
                            || node.state.getCurrentEpoch() != epoch) {
                        node.state.pause();
                        return node.reply(RequestStatus.REQUEST_UNCERTAIN,
                                "No quorum; target unchanged locally, proposal awaits recovery.", request);
                    }
                    node.state.commit(entry.getTag());
                }
                node.print("COMMIT " + tag(entry.getTag()) + " quorum=" + acknowledgments.size() + "/5");
                for (NodeConfig.NodeAddress peer : node.config.getPeers()) {
                    try {
                        node.peer(peer.nodeId()).commit(CommitRequest.newBuilder()
                                .setLeaderId(node.config.getNodeId()).setLeaderEpoch(epoch)
                                .setTag(entry.getTag()).build());
                    } catch (RuntimeException e) {
                        // 暂时没收到 COMMIT 的节点，之后通过心跳触发同步。
                    }
                }
                return committed(entry);
            } catch (IOException | RuntimeException e) {
                node.state.pause();
                return node.reply(RequestStatus.REQUEST_UNCERTAIN, String.valueOf(e.getMessage()), request);
            }
        }
    }

    private OperationResponse committed(LogEntry entry) {
        return node.reply(RequestStatus.REQUEST_COMMITTED, "Target committed.", entry.getRequestId())
                .toBuilder().setCommittedTag(entry.getTag()).build();
    }

    public static String tag(ProposalTag tag) {
        return "(" + tag.getEpoch() + "," + tag.getSeq() + ")";
    }
}
