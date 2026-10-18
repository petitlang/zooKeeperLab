package fr.isep.node;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Node ID, listening port, peer addresses, timeouts, and log location. */
public final class NodeConfig {
    public static final int NODE_COUNT = 5;
    public static final int DEFAULT_BASE_PORT = 5000;
    public static final String HOST = "127.0.0.1";

    private final int nodeId;
    private final List<NodeAddress> cluster;
    private final Path dataDirectory;
    private final boolean bootstrapLeader;

    public NodeConfig(int nodeId) {
        this(nodeId, DEFAULT_BASE_PORT, Path.of("data"));
    }

    public NodeConfig(int nodeId, int basePort, Path dataRoot) {
        this(nodeId, basePort, dataRoot, false);
    }

    private NodeConfig(int nodeId, int basePort, Path dataRoot, boolean bootstrapLeader) {
        if (nodeId < 1 || nodeId > NODE_COUNT) {
            throw new IllegalArgumentException("Node ID must be between 1 and 5.");
        }
        // All five ports must be valid, not just this node's port.
        if (basePort < 0 || basePort > 65535 - NODE_COUNT) {
            throw new IllegalArgumentException("Base port must be between 0 and 65530.");
        }
        this.nodeId = nodeId;
        this.bootstrapLeader = bootstrapLeader;
        this.dataDirectory = Objects.requireNonNull(dataRoot, "dataRoot")
                .toAbsolutePath().normalize().resolve("node-" + nodeId);

        List<NodeAddress> addresses = new ArrayList<>();
        for (int id = 1; id <= NODE_COUNT; id++) {
            addresses.add(new NodeAddress(id, HOST, basePort + id));
        }
        this.cluster = List.copyOf(addresses);
    }

    /** Parse named options; every node must use the same base port. */
    public static NodeConfig fromArgs(String[] args) {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            String name = args[i];
            if (!List.of("--id", "--base-port", "--data-dir", "--bootstrap-leader").contains(name)) {
                throw new IllegalArgumentException("Unknown option: " + name);
            }
            if (i + 1 >= args.length || args[i + 1].startsWith("--")) {
                throw new IllegalArgumentException("Missing value for " + name);
            }
            if (options.putIfAbsent(name, args[i + 1]) != null) {
                throw new IllegalArgumentException("Duplicate option: " + name);
            }
        }
        if (!options.containsKey("--id")) {
            throw new IllegalArgumentException("Required option: --id <1..5>");
        }
        String dataRoot = options.getOrDefault("--data-dir", "data");
        if (dataRoot.isBlank()) {
            throw new IllegalArgumentException("Data directory cannot be blank.");
        }
        String bootstrap = options.getOrDefault("--bootstrap-leader", "false");
        if (!bootstrap.equals("true") && !bootstrap.equals("false")) {
            throw new IllegalArgumentException("--bootstrap-leader must be true or false.");
        }
        return new NodeConfig(
                parseInt("--id", options.get("--id")),
                parseInt("--base-port", options.getOrDefault("--base-port", "5000")),
                Path.of(dataRoot), Boolean.parseBoolean(bootstrap));
    }

    private static int parseInt(String option, String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(option + " must be an integer: " + value, e);
        }
    }

    public int getNodeId() {
        return nodeId;
    }

    public boolean isBootstrapLeader() {
        return bootstrapLeader;
    }

    public int getPort() {
        return cluster.get(nodeId - 1).port();
    }

    public List<NodeAddress> getCluster() {
        return cluster;
    }

    public List<NodeAddress> getPeers() {
        return cluster.stream().filter(address -> address.nodeId() != nodeId).toList();
    }

    public int getQuorum() {
        return NODE_COUNT / 2 + 1;
    }

    public Path getDataDirectory() {
        return dataDirectory;
    }

    public Path getLogFile() {
        return dataDirectory.resolve("entries.log");
    }

    public int getRequiredGuesses() { return 2; }

    public Duration getRpcTimeout() {
        return Duration.ofSeconds(1);
    }

    public Duration getHeartbeatInterval() {
        return Duration.ofMillis(500);
    }

    public Duration getElectionTimeout() {
        return Duration.ofMillis(2500);
    }

    /** A node's address in the fixed localhost cluster. */
    public record NodeAddress(int nodeId, String host, int port) {
        @Override
        public String toString() {
            return "Node " + nodeId + "=" + host + ":" + port;
        }
    }
}
