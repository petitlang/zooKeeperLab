package fr.isep.game;

import fr.isep.coordination.*;
import fr.isep.node.*;
import fr.isep.proto.*;
import io.grpc.StatusRuntimeException;
import java.util.*;

/** Collects two client guesses and submits the winning value. */
public final class GameEngine {
    private final NodeConfig config;
    private final NodeState state;
    private final LeaderCoordinator coordinator;
    private final PeerTransport transport;
    private final Object guessesLock = new Object();
    private final LinkedHashMap<String, GuessRequest> guesses = new LinkedHashMap<>();
    private long round = -1;
    private int epoch = -1;
    private boolean settling;
    private GuessRequest winner;

    public GameEngine(NodeConfig config, NodeState state, LeaderCoordinator coordinator, PeerTransport transport) {
        this.config = config;
        this.state = state;
        this.coordinator = coordinator;
        this.transport = transport;
    }

    public OperationResponse write(WriteRequest request) {
        synchronized (guessesLock) {
            refresh(state.snapshot());
            GuessRequest prior = guesses.get(request.getClientId());
            if (prior != null && prior.getRequestId().equals(request.getRequestId()))
                return OperationResponse.newBuilder().setStatus(RequestStatus.REQUEST_REJECTED)
                        .setRequestId(request.getRequestId()).setState(read())
                        .setMessage("Request ID is already used by a guess.").build();
        }
        OperationResponse result = coordinator.submitWrite(request);
        return result.getState().getNodeId() == config.getNodeId()
                ? result.toBuilder().setState(read()).build() : result;
    }

    public GameState read() {
        synchronized (guessesLock) {
            GameState snapshot = state.snapshot();
            refresh(snapshot);
            return snapshot.toBuilder().setRequiredGuesses(config.getRequiredGuesses())
                    .setReceivedGuesses(guesses.size()).setSettling(settling).build();
        }
    }

    public OperationResponse guess(GuessRequest request) {
        if (!request.hasValue() || request.getValue() < 0 || request.getValue() > 100
                || request.getClientId().isBlank() || request.getRequestId().isBlank())
            return reply(RequestStatus.REQUEST_REJECTED, request, "Expected value 0..100 and nonempty client/request IDs.");
        GameState snapshot = state.snapshot();
        if (snapshot.getRole() != NodeRole.LEADER) {
            if (!snapshot.getReady() || !snapshot.hasLeaderId())
                return reply(RequestStatus.REQUEST_REJECTED, request, "No ready leader is known.");
            try {
                return transport.forward(snapshot.getLeaderId(), ForwardRequest.newBuilder()
                        .setFromNodeId(config.getNodeId()).setGuess(request).build());
            } catch (StatusRuntimeException error) {
                return reply(RequestStatus.REQUEST_UNCERTAIN, request, "Forward failed; read state and retry the same ID.");
            }
        }
        GuessRequest selected;
        int expectedEpoch;
        synchronized (guessesLock) {
            snapshot = state.snapshot();
            refresh(snapshot);
            LogEntry recorded = state.findRequest(request.getClientId(), request.getRequestId()).orElse(null);
            if (recorded != null) {
                if (recorded.getOp() != Operation.GUESS || recorded.getValue() != request.getValue()
                        || recorded.getRoundId() != request.getRoundId() + 1)
                    return reply(RequestStatus.REQUEST_REJECTED, request, "Request ID was used with different data.");
                if (recorded.getStatus() == LogStatus.COMMITTED)
                    return reply(RequestStatus.REQUEST_COMMITTED, request, "Winning guess already committed.")
                            .toBuilder().setCommittedTag(recorded.getTag()).build();
            }
            if (snapshot.getRole() != NodeRole.LEADER || !snapshot.getReady() || !snapshot.hasTarget()
                    || request.getRoundId() != snapshot.getRoundId())
                return reply(RequestStatus.REQUEST_REJECTED, request, "No initialized ready round, or obsolete round.");
            GuessRequest prior = guesses.get(request.getClientId());
            if (prior != null && !prior.equals(request))
                return reply(RequestStatus.REQUEST_REJECTED, request, "One guess per client per round; retry identical data.");
            if ((winner != null || settling) && prior == null)
                return reply(RequestStatus.REQUEST_REJECTED, request, "Round is full; retry a collected request or wait for next round.");
            if (settling) return reply(RequestStatus.REQUEST_ACCEPTED, request, "This round is being committed.");
            guesses.putIfAbsent(request.getClientId(), request);
            if (guesses.size() < config.getRequiredGuesses())
                return reply(RequestStatus.REQUEST_ACCEPTED, request,
                        "Collected " + guesses.size() + "/" + config.getRequiredGuesses() + " guesses.");
            if (winner == null) {
                int target = snapshot.getTarget();
                winner = guesses.values().stream().min(Comparator.comparingInt(
                        value -> Math.abs(value.getValue() - target))).orElseThrow();
            }
            selected = winner;
            expectedEpoch = epoch;
            settling = true;
            System.out.println("[Node " + config.getNodeId() + "] WINNER value=" + selected.getValue()
                    + " round=" + round + " guesses=" + guesses.size());
        }
        // Release the guess lock before replication.
        OperationResponse result;
        try {
            result = coordinator.commitGuess(selected, expectedEpoch);
        } finally {
            synchronized (guessesLock) {
                if (epoch == expectedEpoch && round == selected.getRoundId()) settling = false;
            }
        }
        synchronized (guessesLock) {
            refresh(state.snapshot());
            return result.toBuilder().setRequestId(request.getRequestId()).setState(read())
                    .setMessage("Winner=" + selected.getValue() + ". " + result.getMessage()).build();
        }
    }

    private void refresh(GameState snapshot) {
        int currentEpoch = state.getCurrentEpoch();
        if (round != snapshot.getRoundId() || epoch != currentEpoch || snapshot.getRole() != NodeRole.LEADER) {
            guesses.clear();
            winner = null;
            settling = false;
            round = snapshot.getRoundId();
            epoch = currentEpoch;
        }
    }

    private OperationResponse reply(RequestStatus status, GuessRequest request, String message) {
        return OperationResponse.newBuilder().setStatus(status).setRequestId(request.getRequestId())
                .setMessage(message).setState(read()).build();
    }
}
