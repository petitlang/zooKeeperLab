package fr.isep.node;

import fr.isep.coordination.ElectionManager;
import fr.isep.coordination.LeaderCoordinator;
import fr.isep.game.GameEngine;
import fr.isep.proto.*;
import fr.isep.storage.LogStore;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

// 把节点的几个组件放在一起，统一管理连接和定时线程。
public final class NodeRuntime implements AutoCloseable {
    public final NodeConfig config;
    public final NodeState state;
    public final LogStore log;
    // synchronized 让本节点一次只进行一个写入或选举，不需要显式 lock/unlock。
    public final Object operationLock = new Object();
    public volatile boolean running = true;
    private Thread heartbeatThread;
    public final LeaderCoordinator coordinator;
    public final ElectionManager election;
    public final GameEngine game;
    private final Map<Integer, ManagedChannel> channels = new HashMap<>();

    public NodeRuntime(NodeConfig config, NodeState state, LogStore log) {
        this.config = config;
        this.state = state;
        this.log = log;
        for (NodeConfig.NodeAddress address : config.getPeers()) {
            // 与上次 TP 的 GrpcClient 一样：先 channel，再 blocking stub。
            channels.put(address.nodeId(), ManagedChannelBuilder
                    .forAddress(address.host(), address.port()).usePlaintext().build());
        }
        coordinator = new LeaderCoordinator(this);
        election = new ElectionManager(this);
        game = new GameEngine(this);
    }

    public CoordinationServiceGrpc.CoordinationServiceBlockingStub peer(int id) {
        return CoordinationServiceGrpc.newBlockingStub(channels.get(id))
                .withDeadlineAfter(config.getRpcTimeout().toMillis(), TimeUnit.MILLISECONDS);
    }

    public OperationResponse forward(ForwardRequest request) {
        GameState snapshot = state.snapshot();
        String requestId = request.hasWrite() ? request.getWrite().getRequestId()
                : request.getGuess().getRequestId();
        if (!snapshot.hasLeaderId() || !snapshot.getReady()) {
            return reply(RequestStatus.REQUEST_REJECTED, "Election/recovery in progress; retry later.", requestId);
        }
        try {
            return peer(snapshot.getLeaderId()).withDeadlineAfter(5, TimeUnit.SECONDS).forward(request);
        } catch (RuntimeException e) {
            return reply(RequestStatus.REQUEST_UNCERTAIN, "Leader unreachable; read before retrying.", requestId);
        }
    }

    public OperationResponse reply(RequestStatus status, String message, String requestId) {
        return OperationResponse.newBuilder().setStatus(status).setMessage(message)
                .setRequestId(requestId).setState(state.snapshot()).build();
    }

    public LogSnapshot logSnapshot() {
        synchronized (state) {
            return LogSnapshot.newBuilder().setFromNodeId(config.getNodeId())
                    .setCurrentEpoch(state.getCurrentEpoch()).addAllEntries(log.getEntries()).build();
        }
    }

    public void print(String message) {
        System.out.println("[Node " + config.getNodeId() + "] " + message);
    }

    public void start() {
        // 和 SocketServer 的 Thread 一样；这个线程反复检查心跳和选举。
        heartbeatThread = new Thread(() -> {
            while (running) {
                election.tick();
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        heartbeatThread.start();
    }

    @Override
    public void close() {
        // 等本地写入结束后停止，避免关掉日志时还有线程正在使用它。
        synchronized (operationLock) {
            running = false;
            state.pause();
        }
        if (heartbeatThread != null) {
            heartbeatThread.interrupt();
        }
        for (ManagedChannel channel : channels.values()) {
            channel.shutdownNow();
        }
        try {
            if (heartbeatThread != null) {
                heartbeatThread.join(5000);
            }
            for (ManagedChannel channel : channels.values()) {
                channel.awaitTermination(2, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
