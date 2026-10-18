package fr.isep.coordination;

import fr.isep.node.NodeRuntime;
import fr.isep.proto.*;
import io.grpc.stub.StreamObserver;
import java.io.IOException;

/** Node-to-node gRPC handlers for replication, election, and synchronization. */
public class CoordinationServiceImpl
        extends CoordinationServiceGrpc.CoordinationServiceImplBase {
    private final NodeRuntime node;

    public CoordinationServiceImpl(NodeRuntime node) {
        this.node = node;
    }

    @Override
    public void propose(ProposeRequest request, StreamObserver<Ack> observer) {
        Ack.Builder answer = Ack.newBuilder().setFromNodeId(node.config.getNodeId())
                .setTag(request.getEntry().getTag());
        synchronized (node.state) {
            try {
                node.state.requireLeader(request.getLeaderId(), request.getLeaderEpoch());
                node.state.receiveProposal(request.getEntry()); // 先落盘，再 ACK。
                node.election.heardLeader();
                answer.setAccepted(true).setMessage("Durably logged.");
                node.print("PROPOSE received " + LeaderCoordinator.tag(request.getEntry().getTag()));
            } catch (IOException | RuntimeException e) {
                answer.setAccepted(false).setMessage(String.valueOf(e.getMessage()));
            }
            answer.setCurrentEpoch(node.state.getCurrentEpoch());
        }
        observer.onNext(answer.build());
        observer.onCompleted();
    }

    @Override
    public void commit(CommitRequest request, StreamObserver<NodeResponse> observer) {
        NodeResponse answer;
        synchronized (node.state) {
            try {
                node.state.requireLeader(request.getLeaderId(), request.getLeaderEpoch());
                node.state.commit(request.getTag());
                node.election.heardLeader();
                node.print("COMMIT received " + LeaderCoordinator.tag(request.getTag())
                        + " target=" + node.state.snapshot().getTarget());
                answer = response(true, "Committed.");
            } catch (IOException | RuntimeException e) {
                answer = response(false, String.valueOf(e.getMessage()));
            }
        }
        observer.onNext(answer);
        observer.onCompleted();
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<NodeResponse> observer) {
        NodeResponse answer;
        synchronized (node.state) {
            try {
                node.state.followLeader(request.getLeaderId(), request.getLeaderEpoch());
                node.election.heardLeader();
                boolean caughtUp = node.state.snapshot().getLastCommitted().equals(request.getLastCommitted())
                        && node.state.snapshot().getReady();
                answer = response(caughtUp, caughtUp ? "Heartbeat OK." : "Need log synchronization.");
            } catch (IOException | RuntimeException e) {
                answer = response(false, String.valueOf(e.getMessage()));
            }
        }
        observer.onNext(answer);
        observer.onCompleted();
    }

    @Override
    public void announceLeader(LeaderAnnouncement request, StreamObserver<NodeResponse> observer) {
        NodeResponse answer;
        synchronized (node.state) {
            try {
                node.state.followLeader(request.getLeaderId(), request.getLeaderEpoch());
                node.election.heardLeader();
                node.game.clear();
                node.print("FOLLOW leader=" + request.getLeaderId() + " epoch=" + request.getLeaderEpoch());
                answer = response(true, "Leader announced; waiting for sync.");
            } catch (IOException | RuntimeException e) {
                answer = response(false, String.valueOf(e.getMessage()));
            }
        }
        observer.onNext(answer);
        observer.onCompleted();
    }

    @Override
    public void election(ElectionRequest request, StreamObserver<ElectionResponse> observer) {
        // 交换候选者的最新编号，不把它当作 Raft 投票。
        synchronized (node.state) {
            observer.onNext(ElectionResponse.newBuilder().setFromNodeId(node.config.getNodeId())
                    .setCurrentEpoch(node.state.getCurrentEpoch())
                    .setLatestTag(node.state.getLatestTag().orElse(ProposalTag.getDefaultInstance())).build());
        }
        observer.onCompleted();
    }

    @Override
    public void fetchLog(FetchLogRequest request, StreamObserver<LogSnapshot> observer) {
        observer.onNext(node.logSnapshot());
        observer.onCompleted();
    }

    @Override
    public void syncLog(SyncLogRequest request, StreamObserver<NodeResponse> observer) {
        NodeResponse answer;
        synchronized (node.state) {
            try {
                node.state.requireLeader(request.getLeaderId(), request.getLeaderEpoch());
                node.state.synchronize(request.getEntriesList());
                boolean pending = false;
                for (LogEntry entry : request.getEntriesList()) {
                    pending |= entry.getStatus() == LogStatus.PROPOSED;
                }
                if (!pending) {
                    node.state.markReady();
                }
                node.election.heardLeader();
                node.print("SYNC entries=" + request.getEntriesCount()
                        + " ready=" + node.state.snapshot().getReady());
                answer = response(true, "Log synchronized.");
            } catch (IOException | RuntimeException e) {
                answer = response(false, String.valueOf(e.getMessage()));
            }
        }
        observer.onNext(answer);
        observer.onCompleted();
    }

    @Override
    public void forward(ForwardRequest request, StreamObserver<OperationResponse> observer) {
        // 只在 leader 处理，不继续转发，避免循环。
        OperationResponse answer;
        if (request.hasWrite()) {
            answer = node.game.write(request.getWrite());
        } else if (request.hasGuess()) {
            answer = node.game.guess(request.getGuess());
        } else {
            answer = node.reply(RequestStatus.REQUEST_REJECTED, "Missing operation.", "");
        }
        observer.onNext(answer);
        observer.onCompleted();
    }

    private NodeResponse response(boolean accepted, String message) {
        return NodeResponse.newBuilder().setFromNodeId(node.config.getNodeId())
                .setAccepted(accepted).setCurrentEpoch(node.state.getCurrentEpoch()).setMessage(message).build();
    }
}
