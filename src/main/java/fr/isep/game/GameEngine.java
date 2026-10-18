package fr.isep.game;

import fr.isep.node.NodeRuntime;
import fr.isep.proto.*;
import java.util.LinkedHashMap;
import java.util.Map;

// 第一条 guess 到达后收集三秒，然后选择距离 target 最近的数。
public final class GameEngine {
    public static final int GUESS_WINDOW_SECONDS = 3;
    private final NodeRuntime node;
    private final Map<String, GuessRequest> guesses = new LinkedHashMap<>();
    private long collectingRound;
    private int collectingEpoch;

    public GameEngine(NodeRuntime node) {
        this.node = node;
    }

    public OperationResponse write(WriteRequest request) {
        String error = validate(request.hasValue(), request.getValue(), request.getClientId(), request.getRequestId());
        if (error != null) {
            return node.reply(RequestStatus.REQUEST_REJECTED, error, request.getRequestId());
        }
        synchronized (node.operationLock) {
            OperationResponse response = node.coordinator.write(Operation.WRITE, request.getValue(),
                    request.getClientId(), request.getRequestId());
            if (response.getStatus() == RequestStatus.REQUEST_COMMITTED) {
                clear();
            }
            return response;
        }
    }

    public OperationResponse guess(GuessRequest request) {
        String error = validate(request.hasValue(), request.getValue(), request.getClientId(), request.getRequestId());
        if (error != null) {
            return node.reply(RequestStatus.REQUEST_REJECTED, error, request.getRequestId());
        }
        synchronized (node.operationLock) {
            synchronized (node.state) {
                GameState state = node.state.snapshot();
                if (state.getRole() != NodeRole.LEADER || !state.getReady() || !state.hasTarget()) {
                    return node.reply(RequestStatus.REQUEST_REJECTED, "Set a target on a ready leader first.", request.getRequestId());
                }
                for (LogEntry old : node.log.getEntries()) {
                    if (old.getClientId().equals(request.getClientId()) && old.getRequestId().equals(request.getRequestId())) {
                        if (old.getOp() == Operation.GUESS && old.getValue() == request.getValue()
                                && old.getRoundId() == request.getRoundId() + 1 && old.getStatus() == LogStatus.COMMITTED) {
                            return node.reply(RequestStatus.REQUEST_COMMITTED, "Winning guess already committed.", request.getRequestId())
                                    .toBuilder().setCommittedTag(old.getTag()).build();
                        }
                        return node.reply(RequestStatus.REQUEST_REJECTED, "Request ID already used.", request.getRequestId());
                    }
                }
                if (request.getRoundId() != state.getRoundId()) {
                    return node.reply(RequestStatus.REQUEST_REJECTED, "Old round; read and guess again.", request.getRequestId());
                }
                synchronized (this) {
                    // 每个客户端每轮最多一个猜数，便于多个客户端公平演示。
                    GuessRequest previous = guesses.get(request.getClientId());
                    if (previous != null) {
                        return node.reply(previous.equals(request) ? RequestStatus.REQUEST_ACCEPTED : RequestStatus.REQUEST_REJECTED,
                                "This client already submitted a guess in this round.", request.getRequestId());
                    }
                    boolean first = guesses.isEmpty();
                    guesses.put(request.getClientId(), request);
                    node.print("GUESS client=" + request.getClientId() + " value=" + request.getValue()
                            + " round=" + request.getRoundId());
                    if (first) {
                        collectingRound = state.getRoundId();
                        collectingEpoch = node.state.getCurrentEpoch();
                        long round = collectingRound;
                        int epoch = collectingEpoch;
                        // 与上个 Socket TP 一样用普通 Thread，等三秒后处理本轮结果。
                        Thread roundThread = new Thread(() -> {
                            try {
                                Thread.sleep(GUESS_WINDOW_SECONDS * 1000L);
                                finishRound(round, epoch);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });
                        roundThread.setDaemon(true); // 不阻止节点进程退出。
                        roundThread.start();
                    }
                }
                return node.reply(RequestStatus.REQUEST_ACCEPTED, "Guess queued; result after 3 seconds. Use read.", request.getRequestId());
            }
        }
    }

    private void finishRound(long round, int epoch) {
        synchronized (node.operationLock) {
            if (!node.running) { return; }
            GameState state = node.state.snapshot();
            int currentEpoch = node.state.getCurrentEpoch();
            GuessRequest winner = null;
            synchronized (this) {
                if (round != collectingRound || epoch != collectingEpoch || guesses.isEmpty()) {
                    return;
                }
                if (state.getRole() != NodeRole.LEADER || !state.getReady()
                        || state.getRoundId() != round || currentEpoch != epoch) {
                    clear();
                    return;
                }
                for (GuessRequest guess : guesses.values()) {
                    // 同距离时保留先到达的猜数。
                    if (winner == null || Math.abs(guess.getValue() - state.getTarget())
                            < Math.abs(winner.getValue() - state.getTarget())) {
                        winner = guess;
                    }
                }
                clear();
            }
            node.print("WINNER round=" + round + " value=" + winner.getValue());
            OperationResponse result = node.coordinator.write(Operation.GUESS, winner.getValue(),
                    winner.getClientId(), winner.getRequestId());
            node.print("ROUND result=" + result.getStatus());
        }
    }

    public synchronized void clear() {
        guesses.clear();
        collectingRound = 0;
        collectingEpoch = 0;
    }

    private static String validate(boolean present, int value, String client, String request) {
        if (!present || value < 0 || value > 100) {
            return "Value must be present and between 0 and 100.";
        }
        return client.isBlank() || request.isBlank() ? "Client ID and request ID are required." : null;
    }
}
