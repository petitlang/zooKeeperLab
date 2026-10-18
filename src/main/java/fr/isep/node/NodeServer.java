package fr.isep.node;

import java.io.IOException;

/** Process entrypoint. Runtime owns composition and resource shutdown. */
public final class NodeServer implements AutoCloseable {
    private final NodeRuntime runtime;

    public NodeServer(NodeConfig config) throws IOException { this(config, true); }

    /** Disable background election only for deterministic protocol tests. */
    public NodeServer(NodeConfig config, boolean automaticElection) throws IOException {
        runtime = new NodeRuntime(config, automaticElection);
    }

    public void start() throws IOException { runtime.start(); }
    public void bootstrapLeader() throws IOException { runtime.bootstrapLeader(); }
    public void awaitTermination() throws InterruptedException { runtime.awaitTermination(); }
    @Override public void close() { runtime.close(); }

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
        System.out.println("Usage: NodeServer --id <1..5> [--base-port <5000>] [--data-dir <data>]"
                + " [--bootstrap-leader <true|false>]");
        System.out.println("Port = base port + node ID. Use the same base port for all five nodes.");
        System.out.println("Each node stores data in <data-dir>/node-<id>.");
        System.out.println("Automatic election is enabled by default. Manual bootstrap is only for fresh-cluster development.");
    }
}
