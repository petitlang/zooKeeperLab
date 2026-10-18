package fr.isep.coordination;

import fr.isep.node.NodeConfig;
import fr.isep.game.GameEngine;
import fr.isep.node.NodeState;
import fr.isep.proto.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.Objects;

/** Node-to-node gRPC handlers for replication, election, and synchronization. */
public final class CoordinationServiceImpl
        extends CoordinationServiceGrpc.CoordinationServiceImplBase {
    private final NodeState state;
    private final GameEngine game;
    private final ElectionManager election;

    public CoordinationServiceImpl(NodeState state, GameEngine game, ElectionManager election) {
        this.state = Objects.requireNonNull(state, "state");
        this.game = Objects.requireNonNull(game);
        this.election = Objects.requireNonNull(election);
    }

    @Override
    public void propose(ProposeRequest request, StreamObserver<Ack> observer) {
        Ack.Builder reply = Ack.newBuilder().setFromNodeId(nodeId());
        if (request.hasEntry() && request.getEntry().hasTag()) {
            reply.setTag(request.getEntry().getTag());
        }
        try {
            synchronized (state) {
                requireLeader(request.getLeaderId(), request.getLeaderEpoch());
                if (!request.hasEntry() || request.getEntry().getStatus() != LogStatus.PROPOSED) {
                    throw new IllegalArgumentException("Expected a PROPOSED entry.");
                }
                // This call forces the record to disk before a positive ACK is built.
                state.receiveProposal(request.getEntry());
                reply.setAccepted(true).setCurrentEpoch(state.getCurrentEpoch())
                        .setMessage("Proposal durably logged.");
            }
            trace("ACK", request.getEntry().getTag());
        } catch (IllegalArgumentException | IllegalStateException e) {
            reply.setAccepted(false).setCurrentEpoch(state.getCurrentEpoch()).setMessage(e.getMessage());
        } catch (IOException e) {
            storageFailure(observer, e);
            return;
        }
        observer.onNext(reply.build());
        observer.onCompleted();
    }

    @Override
    public void commit(CommitRequest request, StreamObserver<NodeResponse> observer) {
        try {
            synchronized (state) {
                requireLeader(request.getLeaderId(), request.getLeaderEpoch());
                if (!request.hasTag() || request.getTag().getEpoch() > request.getLeaderEpoch()) {
                    throw new IllegalArgumentException("Invalid commit tag.");
                }
                state.commit(request.getTag());
                resumeIfResolved();
            }
            trace("COMMIT", request.getTag());
            respond(observer, true, "Commit durably applied.");
        } catch (IllegalArgumentException | IllegalStateException e) {
            respond(observer, false, e.getMessage());
        } catch (IOException e) {
            storageFailure(observer, e);
        }
    }

    @Override
    public void announceLeader(LeaderAnnouncement request, StreamObserver<NodeResponse> observer) {
        try {
            synchronized (state) {
                state.followLeader(request.getLeaderId(), request.getLeaderEpoch());
                election.leaderContact();
            }
            respond(observer, true, "Leader recorded; pending log records still require synchronization.");
        } catch (IllegalArgumentException | IllegalStateException e) {
            respond(observer, false, e.getMessage());
        } catch (IOException e) {
            storageFailure(observer, e);
        }
    }

    @Override
    public void fetchLog(FetchLogRequest request, StreamObserver<LogSnapshot> observer) {
        if (!clusterId(request.getFromNodeId())) {
            observer.onError(Status.INVALID_ARGUMENT.withDescription("Invalid requesting node ID.")
                    .asRuntimeException());
            return;
        }
        try {
            observer.onNext(state.logSnapshot());
            observer.onCompleted();
        } catch (IllegalStateException e) {
            observer.onError(Status.UNAVAILABLE.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void forward(ForwardRequest request, StreamObserver<OperationResponse> observer) {
        if (!clusterId(request.getFromNodeId()) || (!request.hasWrite() && !request.hasGuess())) {
            observer.onError(Status.INVALID_ARGUMENT
                    .withDescription("Expected a cluster node forwarding Write or Guess.")
                    .asRuntimeException());
            return;
        }
        // Only the destination leader handles Forward, avoiding forwarding loops.
        if (state.snapshot().getRole() != NodeRole.LEADER) {
            observer.onError(Status.FAILED_PRECONDITION.withDescription("This node is not the leader.")
                    .asRuntimeException());
            return;
        }
        observer.onNext(request.hasWrite() ? game.write(request.getWrite()) : game.guess(request.getGuess()));
        observer.onCompleted();
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<NodeResponse> observer) {
        try {
            synchronized (state) {
                requireLeader(request.getLeaderId(), request.getLeaderEpoch());
                election.leaderContact();
                GameState local = state.snapshot();
                if (local.hasLastCommitted() != request.hasLastCommitted()
                        || !local.getLastCommitted().equals(request.getLastCommitted())) state.pause();
            }
            respond(observer, true, "Heartbeat received.");
        } catch (IllegalArgumentException | IllegalStateException error) {
            respond(observer, false, error.getMessage());
        } catch (IOException error) { storageFailure(observer, error); }
    }

    @Override
    public void election(ElectionRequest request, StreamObserver<ElectionResponse> observer) {
        if (!clusterId(request.getFromNodeId())) {
            observer.onError(Status.INVALID_ARGUMENT.asRuntimeException());
            return;
        }
        synchronized (state) {
            ElectionResponse.Builder reply = ElectionResponse.newBuilder().setFromNodeId(nodeId())
                    .setCurrentEpoch(state.getCurrentEpoch());
            state.getLatestTag().ifPresent(reply::setLatestTag);
            observer.onNext(reply.build());
        }
        observer.onCompleted();
    }

    @Override
    public void syncLog(SyncLogRequest request, StreamObserver<NodeResponse> observer) {
        try {
            synchronized (state) {
                requireLeader(request.getLeaderId(), request.getLeaderEpoch());
                state.installRecoveredLog(request.getLeaderId(), request.getLeaderEpoch(), request.getEntriesList());
                election.leaderContact();
                resumeIfResolved();
            }
            respond(observer, true, "Log checkpoint durably synchronized.");
        } catch (IllegalArgumentException | IllegalStateException error) {
            respond(observer, false, error.getMessage());
        } catch (IOException error) { storageFailure(observer, error); }
    }

    /** Called while holding the state lock, so epoch validation and logging are atomic. */
    private void requireLeader(int id, int epoch) throws IOException {
        if (!clusterId(id) || id == nodeId() || epoch <= 0) {
            throw new IllegalArgumentException("Invalid sending leader.");
        }
        if (epoch > state.getCurrentEpoch()) {
            state.observeHigherEpoch(epoch);
        }
        GameState snapshot = state.snapshot();
        if (epoch != state.getCurrentEpoch() || snapshot.getRole() != NodeRole.FOLLOWER
                || !snapshot.hasLeaderId() || snapshot.getLeaderId() != id) {
            throw new IllegalArgumentException("Sender is not the recognized leader of this epoch.");
        }
    }

    private void resumeIfResolved() {
        if (!state.snapshot().getReady() && state.logSnapshot().getEntriesList().stream()
                .noneMatch(entry -> entry.getStatus() == LogStatus.PROPOSED)) {
            state.markReady();
        }
    }

    private void respond(StreamObserver<NodeResponse> observer, boolean accepted, String message) {
        observer.onNext(NodeResponse.newBuilder().setFromNodeId(nodeId()).setAccepted(accepted)
                .setCurrentEpoch(state.getCurrentEpoch()).setMessage(message).build());
        observer.onCompleted();
    }

    private <T> void storageFailure(StreamObserver<T> observer, IOException error) {
        state.pause();
        observer.onError(Status.INTERNAL.withDescription("Persistent log write failed.")
                .withCause(error).asRuntimeException());
    }

    private int nodeId() {
        return state.snapshot().getNodeId();
    }

    private static boolean clusterId(int id) {
        return id >= 1 && id <= NodeConfig.NODE_COUNT;
    }

    private void trace(String action, ProposalTag tag) {
        System.out.println("[Node " + nodeId() + "] " + action
                + " (" + tag.getEpoch() + "," + tag.getSeq() + ")");
    }
}
