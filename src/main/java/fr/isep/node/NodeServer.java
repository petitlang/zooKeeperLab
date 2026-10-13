package fr.isep.node;

import fr.isep.coordination.CoordinationServiceImpl;
import fr.isep.game.GameServiceImpl;
import fr.isep.proto.CoordinationServiceGrpc;
import fr.isep.proto.GameServiceGrpc;
import fr.isep.proto.GameState;
import fr.isep.proto.LogStatus;
import fr.isep.storage.LogStore;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Node startup, gRPC service registration, and shutdown. */
public final class NodeServer implements AutoCloseable {
    private final NodeConfig config;
    private final Server server;
    private final LogStore logStore;
    private final NodeState state;

    public NodeServer(NodeConfig config) throws IOException {
        this.config = Objects.requireNonNull(config, "config");
        logStore = new LogStore(config.getLogFile());
        try {
            state = new NodeState(config, logStore);
            // Both APIs share one listening port, just as multiple Lab2 services can.
            server = ServerBuilder.forPort(config.getPort())
                    .addService(new GameServiceImpl())
                    .addService(new CoordinationServiceImpl())
                    .build();
        } catch (RuntimeException | Error e) {
            try {
                logStore.close();
            } catch (IOException closeError) {
                e.addSuppressed(closeError);
            }
            throw e;
        }
    }

    public void start() throws IOException {
        server.start();

        String prefix = "[Node " + config.getNodeId() + "] ";
        System.out.println(prefix + "Started on " + NodeConfig.HOST + ":" + server.getPort());
        System.out.println(prefix + "Registered: " + GameServiceGrpc.getServiceDescriptor().getName());
        System.out.println(prefix + "Registered: " + CoordinationServiceGrpc.getServiceDescriptor().getName());
        System.out.println(prefix + "Peers: " + config.getPeers());
        System.out.println(prefix + "Quorum: " + config.getQuorum() + "/" + NodeConfig.NODE_COUNT);
        System.out.println(prefix + "Data directory: " + config.getDataDirectory());
        GameState restored = state.snapshot();
        long pending = logStore.getEntries().stream()
                .filter(entry -> entry.getStatus() == LogStatus.PROPOSED).count();
        System.out.println(prefix + "Restored: epoch=" + state.getCurrentEpoch()
                + " seq=" + state.getCurrentSeq()
                + " target=" + (restored.hasTarget() ? restored.getTarget() : "unset")
                + " round=" + restored.getRoundId() + " pending=" + pending);
        System.out.println(prefix + "Startup only: RPC handlers and leader election are not implemented yet.");
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    /** Stop accepting calls, allow existing calls to finish, then force shutdown. */
    @Override
    public void close() {
        server.shutdown();
        try {
            if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                server.shutdownNow();
                server.awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            server.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            try {
                logStore.close();
            } catch (IOException e) {
                System.err.println("[Node " + config.getNodeId() + "] Log close failed: " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        if (args.length == 1 && "--help".equals(args[0])) {
            printUsage();
            return;
        }

        NodeConfig config;
        try {
            config = NodeConfig.fromArgs(args);
        } catch (IllegalArgumentException e) {
            System.err.println("Configuration error: " + e.getMessage());
            printUsage();
            System.exit(2);
            return;
        }

        try (NodeServer node = new NodeServer(config)) {
            node.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.err.println("[Node " + config.getNodeId() + "] Shutting down...");
                node.close();
            }, "node-" + config.getNodeId() + "-shutdown"));
            // Keep this node process alive until it is stopped (for example Ctrl+C).
            node.awaitTermination();
        } catch (IOException e) {
            System.err.println("[Node " + config.getNodeId() + "] Startup failed on port "
                    + config.getPort() + ": " + e.getMessage());
            System.exit(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void printUsage() {
        System.out.println("Usage: NodeServer --id <1..5> [--base-port <5000>] [--data-dir <data>]");
        System.out.println("Port = base port + node ID. Use the same base port for all five nodes.");
        System.out.println("Each node stores data in <data-dir>/node-<id>.");
    }
}
