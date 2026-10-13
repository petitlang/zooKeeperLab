package fr.isep.node;

import fr.isep.coordination.CoordinationServiceImpl;
import fr.isep.game.GameServiceImpl;
import fr.isep.proto.CoordinationServiceGrpc;
import fr.isep.proto.GameServiceGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Node startup, gRPC service registration, and shutdown. */
public final class NodeServer implements AutoCloseable {
    private final NodeConfig config;
    private final Server server;

    public NodeServer(NodeConfig config) {
        this.config = Objects.requireNonNull(config, "config");
        // Both APIs share one listening port, just as multiple Lab2 services can.
        this.server = ServerBuilder.forPort(config.getPort())
                .addService(new GameServiceImpl())
                .addService(new CoordinationServiceImpl())
                .build();
    }

    public void start() throws IOException {
        Files.createDirectories(config.getDataDirectory());
        server.start();

        String prefix = "[Node " + config.getNodeId() + "] ";
        System.out.println(prefix + "Started on " + NodeConfig.HOST + ":" + server.getPort());
        System.out.println(prefix + "Registered: " + GameServiceGrpc.getServiceDescriptor().getName());
        System.out.println(prefix + "Registered: " + CoordinationServiceGrpc.getServiceDescriptor().getName());
        System.out.println(prefix + "Peers: " + config.getPeers());
        System.out.println(prefix + "Quorum: " + config.getQuorum() + "/" + NodeConfig.NODE_COUNT);
        System.out.println(prefix + "Data directory: " + config.getDataDirectory());
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
