package fr.isep.coordination;

import fr.isep.node.NodeConfig;
import fr.isep.proto.*;
import io.grpc.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiFunction;

/** Owns reusable channels and bounded concurrent RPCs; contains no consensus policy. */
public final class PeerTransport implements AutoCloseable {
    private final NodeConfig config;
    private final Map<Integer, ManagedChannel> channels = new HashMap<>();
    private final ExecutorService pool;
    private volatile boolean closed;

    public PeerTransport(NodeConfig config) {
        this.config = Objects.requireNonNull(config);
        pool = Executors.newFixedThreadPool(8, task -> {
            Thread thread = new Thread(task, "node-" + config.getNodeId() + "-rpc");
            thread.setDaemon(true);
            return thread;
        });
        try {
            for (NodeConfig.NodeAddress peer : config.getPeers()) {
                channels.put(peer.nodeId(), ManagedChannelBuilder.forAddress(peer.host(), peer.port())
                        .usePlaintext().build());
            }
        } catch (RuntimeException | Error error) {
            close();
            throw error;
        }
    }

    /** A transport error belongs to one peer and never aborts other peer calls. */
    public record Reply<T>(int nodeId, T value, String error) {}

    public <T> List<Reply<T>> callAll(BiFunction<NodeConfig.NodeAddress,
            CoordinationServiceGrpc.CoordinationServiceBlockingStub, T> operation) {
        if (closed) return List.of();
        Deadline deadline = Deadline.after(config.getRpcTimeout().toNanos(), TimeUnit.NANOSECONDS);
        CompletionService<Reply<T>> completed = new ExecutorCompletionService<>(pool);
        List<Future<Reply<T>>> pending = new ArrayList<>();
        List<Reply<T>> replies = new ArrayList<>();
        try {
            for (NodeConfig.NodeAddress peer : config.getPeers()) {
                ManagedChannel channel = channels.get(peer.nodeId());
                if (channel.getState(false) == ConnectivityState.TRANSIENT_FAILURE) channel.resetConnectBackoff();
                pending.add(completed.submit(() -> {
                    try {
                        T value = operation.apply(peer, CoordinationServiceGrpc.newBlockingStub(channel)
                                .withWaitForReady().withDeadline(deadline));
                        return new Reply<>(peer.nodeId(), value, null);
                    } catch (RuntimeException error) {
                        return new Reply<>(peer.nodeId(), null, error.toString());
                    }
                }));
            }
            for (int i = 0; i < pending.size(); i++) {
                long remaining = deadline.timeRemaining(TimeUnit.NANOSECONDS);
                if (remaining <= 0) break;
                Future<Reply<T>> result = completed.poll(remaining, TimeUnit.NANOSECONDS);
                if (result == null) break;
                replies.add(result.get());
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException error) {
            throw new IllegalStateException("Peer task failed", error.getCause());
        } catch (RejectedExecutionException error) {
            if (!closed) throw error;
        } finally {
            pending.forEach(future -> future.cancel(true));
        }
        return List.copyOf(replies);
    }

    public OperationResponse forward(int leaderId, ForwardRequest request) {
        ManagedChannel channel = channels.get(leaderId);
        if (closed || channel == null) throw Status.UNAVAILABLE.withDescription("No leader channel").asRuntimeException();
        return CoordinationServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(5, TimeUnit.SECONDS).forward(request);
    }

    @Override public void close() {
        closed = true;
        pool.shutdownNow();
        channels.values().forEach(ManagedChannel::shutdownNow);
        try {
            pool.awaitTermination(3, TimeUnit.SECONDS);
            for (ManagedChannel channel : channels.values()) channel.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException error) { Thread.currentThread().interrupt(); }
    }
}
