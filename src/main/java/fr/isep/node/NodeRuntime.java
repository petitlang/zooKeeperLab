package fr.isep.node;

import fr.isep.coordination.*;
import fr.isep.game.*;
import fr.isep.storage.LogStore;
import io.grpc.*;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Creates node components and closes their resources. */
public final class NodeRuntime implements AutoCloseable {
    private final NodeConfig config;
    private final boolean automatic;
    private final LogStore store;
    private final PeerTransport transport;
    private final LeaderCoordinator coordinator;
    private final ElectionManager election;
    private final Server server;
    private final AtomicBoolean closed = new AtomicBoolean();

    public NodeRuntime(NodeConfig config, boolean automatic) throws IOException {
        this.config = config;
        this.automatic = automatic;
        store = new LogStore(config.getLogFile());
        PeerTransport createdTransport = null;
        ElectionManager createdElection = null;
        try {
            NodeState state = new NodeState(config, store);
            transport = createdTransport = new PeerTransport(config);
            coordinator = new LeaderCoordinator(config, state, transport);
            RecoveryManager recovery = new RecoveryManager(config, state, transport, coordinator);
            election = createdElection = new ElectionManager(config, state, transport, recovery);
            GameEngine game = new GameEngine(config, state, coordinator, transport);
            server = ServerBuilder.forPort(config.getPort()).addService(new GameServiceImpl(game))
                    .addService(new CoordinationServiceImpl(state, game, election)).build();
        } catch (RuntimeException | Error error) {
            if (createdElection != null) createdElection.close();
            if (createdTransport != null) createdTransport.close();
            try { store.close(); } catch (IOException failure) { error.addSuppressed(failure); }
            throw error;
        }
    }

    public void start() throws IOException {
        try {
            server.start();
            System.out.println("[Node " + config.getNodeId() + "] Started port=" + config.getPort()
                    + " quorum=3/5 log=" + config.getLogFile());
            if (config.isBootstrapLeader()) coordinator.bootstrapLeader();
            if (automatic) election.start();
        } catch (IOException | RuntimeException error) {
            close();
            throw error;
        }
    }

    public void bootstrapLeader() throws IOException { coordinator.bootstrapLeader(); }
    public void awaitTermination() throws InterruptedException { server.awaitTermination(); }

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        election.close();
        server.shutdown();
        try {
            if (!server.awaitTermination(6, TimeUnit.SECONDS)) {
                server.shutdownNow();
                server.awaitTermination(3, TimeUnit.SECONDS);
            }
        } catch (InterruptedException error) {
            server.shutdownNow();
            Thread.currentThread().interrupt();
        }
        coordinator.close();
        transport.close();
        try { store.close(); }
        catch (IOException error) { System.err.println("Log close failed: " + error.getMessage()); }
    }
}
