package fr.isep.game;

import fr.isep.node.NodeRuntime;
import fr.isep.proto.*;
import io.grpc.stub.StreamObserver;

/** Client-facing gRPC handlers for Write, Guess, and Read. */
public class GameServiceImpl extends GameServiceGrpc.GameServiceImplBase {
    private final NodeRuntime node;

    public GameServiceImpl(NodeRuntime node) {
        this.node = node;
    }

    @Override
    public void write(WriteRequest request, StreamObserver<OperationResponse> observer) {
        OperationResponse response;
        if (node.state.snapshot().getRole() == NodeRole.LEADER) {
            response = node.game.write(request);
        } else {
            node.print("FORWARD WRITE to leader");
            response = node.forward(ForwardRequest.newBuilder().setFromNodeId(node.config.getNodeId())
                    .setWrite(request).build());
        }
        // 与 CalculatorServiceImpl 相同的返回方式。
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void guess(GuessRequest request, StreamObserver<OperationResponse> observer) {
        OperationResponse response;
        if (node.state.snapshot().getRole() == NodeRole.LEADER) {
            response = node.game.guess(request);
        } else {
            node.print("FORWARD GUESS to leader");
            response = node.forward(ForwardRequest.newBuilder().setFromNodeId(node.config.getNodeId())
                    .setGuess(request).build());
        }
        observer.onNext(response);
        observer.onCompleted();
    }

    @Override
    public void read(ReadRequest request, StreamObserver<GameState> observer) {
        // 本地读：显示这个节点已提交的状态，离线 follower 可能暂时落后。
        observer.onNext(node.state.snapshot());
        observer.onCompleted();
    }
}
