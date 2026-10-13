package fr.isep.game;

import fr.isep.coordination.LeaderCoordinator;
import fr.isep.node.NodeState;
import fr.isep.proto.*;
import io.grpc.stub.StreamObserver;

import java.util.Objects;

/** Client-facing gRPC handlers for Write, Guess, and Read. */
public final class GameServiceImpl extends GameServiceGrpc.GameServiceImplBase {
    private final NodeState state;
    private final LeaderCoordinator coordinator;

    public GameServiceImpl(NodeState state, LeaderCoordinator coordinator) {
        this.state = Objects.requireNonNull(state, "state");
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
    }

    @Override
    public void write(WriteRequest request, StreamObserver<OperationResponse> observer) {
        observer.onNext(coordinator.submitWrite(request));
        observer.onCompleted();
    }

    @Override
    public void read(ReadRequest request, StreamObserver<GameState> observer) {
        observer.onNext(state.snapshot());
        observer.onCompleted();
    }
}
