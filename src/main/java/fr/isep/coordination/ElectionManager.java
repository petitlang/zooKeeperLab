package fr.isep.coordination;

import fr.isep.node.*;
import fr.isep.proto.*;
import java.io.IOException;
import java.util.concurrent.*;

/** Sends heartbeats and starts recovery after a timeout. */
public final class ElectionManager implements AutoCloseable {
    private final NodeConfig config;
    private final NodeState state;
    private final PeerTransport transport;
    private final RecoveryManager recovery;
    private final ScheduledExecutorService worker;
    private volatile long lastContact = System.nanoTime();
    private volatile boolean closed;
    private int ticks;

    public ElectionManager(NodeConfig config, NodeState state, PeerTransport transport, RecoveryManager recovery) {
        this.config = config;
        this.state = state;
        this.transport = transport;
        this.recovery = recovery;
        worker = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "node-" + config.getNodeId() + "-election");
            thread.setDaemon(true);
            return thread;
        });
    }

    public void start() {
        lastContact = System.nanoTime();
        worker.scheduleWithFixedDelay(this::tick, config.getHeartbeatInterval().toMillis(),
                config.getHeartbeatInterval().toMillis(), TimeUnit.MILLISECONDS);
    }

    /** Record contact after validating the leader. */
    void leaderContact() { lastContact = System.nanoTime(); }

    private void tick() {
        if (closed) return;
        try {
            GameState snapshot = state.snapshot();
            if (snapshot.getRole() == NodeRole.LEADER && snapshot.getReady()) {
                int epoch = state.getCurrentEpoch();
                HeartbeatRequest.Builder heartbeat = HeartbeatRequest.newBuilder().setLeaderId(config.getNodeId())
                        .setLeaderEpoch(epoch);
                if (snapshot.hasLastCommitted()) heartbeat.setLastCommitted(snapshot.getLastCommitted());
                for (PeerTransport.Reply<NodeResponse> reply : transport.callAll((peer, stub) -> stub.heartbeat(heartbeat.build()))) {
                    if (reply.error() == null && reply.value().getFromNodeId() == reply.nodeId())
                        state.observeHigherEpoch(reply.value().getCurrentEpoch());
                }
                if (++ticks % 2 == 0) recovery.catchUpFollowers();
            } else if (System.nanoTime() - lastContact > config.getElectionTimeout().toNanos()
                    + TimeUnit.MILLISECONDS.toNanos(config.getNodeId() * 350L)) {
                recovery.electAndRecover();
                lastContact = System.nanoTime();
            }
        } catch (IOException | RuntimeException error) {
            state.pause();
            lastContact = System.nanoTime();
            System.err.println("[Node " + config.getNodeId() + "] Election/recovery: " + error.getMessage());
        }
    }

    @Override public void close() {
        closed = true;
        worker.shutdownNow();
        try { worker.awaitTermination(5, TimeUnit.SECONDS); }
        catch (InterruptedException error) { Thread.currentThread().interrupt(); }
    }
}
