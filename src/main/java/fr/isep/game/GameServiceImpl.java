package fr.isep.game;

import fr.isep.proto.*;
import io.grpc.stub.StreamObserver;
import java.util.Objects;

/** Client RPC adapter; all game rules belong to GameEngine. */
public final class GameServiceImpl extends GameServiceGrpc.GameServiceImplBase {
    private final GameEngine game;
    public GameServiceImpl(GameEngine game) { this.game = Objects.requireNonNull(game); }

    @Override public void write(WriteRequest request, StreamObserver<OperationResponse> observer) {
        observer.onNext(game.write(request)); observer.onCompleted();
    }
    @Override public void guess(GuessRequest request, StreamObserver<OperationResponse> observer) {
        observer.onNext(game.guess(request)); observer.onCompleted();
    }
    @Override public void read(ReadRequest request, StreamObserver<GameState> observer) {
        observer.onNext(game.read()); observer.onCompleted();
    }
}
