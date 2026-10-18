package fr.isep.client;

import fr.isep.proto.*;
import io.grpc.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Interactive client. Retry preserves the exact request ID, value and original round. */
public final class ClientApp {
    private ClientApp() {}

    public static void main(String[] args) {
        try {
            Map<String, String> options = parse(args);
            int node = Integer.parseInt(options.getOrDefault("--node", "1"));
            int base = Integer.parseInt(options.getOrDefault("--base-port", "5000"));
            if (base < 0 || base > 65530) throw new IllegalArgumentException("Invalid base port.");
            String client = options.getOrDefault("--client", "client-" + UUID.randomUUID());
            if (client.isBlank()) throw new IllegalArgumentException("Empty client ID.");
            try (Session session = new Session(node, base, client)) {
                if (options.containsKey("--command")) {
                    session.execute(options.get("--command"));
                    return;
                }
                System.out.println("Commands: write N, guess N, read, retry, connect NODE, help, quit");
                try (Scanner input = new Scanner(System.in)) {
                    while (input.hasNextLine()) {
                        String command = input.nextLine().trim();
                        if (command.equalsIgnoreCase("quit")) break;
                        session.execute(command);
                    }
                }
            }
        } catch (IllegalArgumentException error) {
            System.err.println(error.getMessage());
            System.err.println("Usage: ClientApp [--node 1..5] [--client ID] [--base-port 5000] [--command \"read\"]");
            System.exit(2);
        }
    }

    private static Map<String, String> parse(String[] args) {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            if (!Set.of("--node", "--client", "--base-port", "--command").contains(args[i])
                    || i + 1 == args.length || options.putIfAbsent(args[i], args[i + 1]) != null)
                throw new IllegalArgumentException("Invalid client options.");
        }
        return options;
    }

    private static final class Session implements AutoCloseable {
        private final int base;
        private final String client;
        private ManagedChannel channel;
        private WriteRequest pendingWrite;
        private GuessRequest pendingGuess;

        private Session(int node, int base, String client) {
            this.base = base;
            this.client = client;
            connect(node);
        }

        private void connect(int node) {
            if (node < 1 || node > 5) throw new IllegalArgumentException("Node must be 1..5.");
            close();
            channel = ManagedChannelBuilder.forAddress("127.0.0.1", base + node).usePlaintext().build();
            System.out.println("Connected node=" + node + " client=" + client);
        }

        private GameServiceGrpc.GameServiceBlockingStub stub() {
            return GameServiceGrpc.newBlockingStub(channel).withDeadlineAfter(6, TimeUnit.SECONDS);
        }

        private void execute(String command) {
            try {
                String[] words = command.trim().split("\\s+");
                switch (words[0].toLowerCase(Locale.ROOT)) {
                    case "read" -> show(stub().read(ReadRequest.getDefaultInstance()));
                    case "connect" -> { requireArgument(words); connect(Integer.parseInt(words[1])); }
                    case "write", "guess" -> {
                        requireArgument(words);
                        int value = Integer.parseInt(words[1]);
                        if (value < 0 || value > 100) throw new IllegalArgumentException("Value must be 0..100.");
                        String request = UUID.randomUUID().toString();
                        if (words[0].equalsIgnoreCase("write")) {
                            pendingWrite = WriteRequest.newBuilder().setClientId(client).setRequestId(request).setValue(value).build();
                            pendingGuess = null;
                        } else {
                            GameState current = stub().read(ReadRequest.getDefaultInstance());
                            if (!current.hasTarget() || !current.getReady()) {
                                show(current);
                                System.out.println("No initialized ready round; try read again.");
                                return;
                            }
                            pendingGuess = GuessRequest.newBuilder().setClientId(client).setRequestId(request)
                                    .setValue(value).setRoundId(current.getRoundId()).build();
                            pendingWrite = null;
                        }
                        retry();
                    }
                    case "retry" -> retry();
                    case "help" -> System.out.println("write N | guess N | read | retry | connect NODE | quit");
                    default -> System.out.println("Unknown command; use help.");
                }
            } catch (StatusRuntimeException error) {
                System.out.println("RPC " + error.getStatus().getCode()
                        + ": request retained; use read/retry, or connect NODE if this node stopped.");
            } catch (IllegalArgumentException error) { System.out.println(error.getMessage()); }
        }

        private void retry() {
            if (pendingWrite == null && pendingGuess == null) {
                System.out.println("No retained request.");
                return;
            }
            String id = pendingWrite != null ? pendingWrite.getRequestId() : pendingGuess.getRequestId();
            System.out.println("request=" + id);
            OperationResponse result = pendingWrite != null ? stub().write(pendingWrite) : stub().guess(pendingGuess);
            System.out.println(result.getStatus() + " " + result.getMessage());
            show(result.getState());
            if (result.getStatus() == RequestStatus.REQUEST_COMMITTED) {
                pendingWrite = null;
                pendingGuess = null;
            } else if (result.getStatus() == RequestStatus.REQUEST_REJECTED) {
                System.out.println("Request retained; read state, retry unchanged, or issue a new command.");
            }
        }

        private static void requireArgument(String[] words) {
            if (words.length != 2) throw new IllegalArgumentException("Expected one numeric argument.");
        }

        private static void show(GameState state) {
            System.out.println("node=" + state.getNodeId() + " leader=" + (state.hasLeaderId() ? state.getLeaderId() : "unknown")
                    + " role=" + state.getRole() + " ready=" + state.getReady()
                    + " target=" + (state.hasTarget() ? state.getTarget() : "unset") + " round=" + state.getRoundId()
                    + " last=" + state.getLastCommitted().getEpoch() + ":" + state.getLastCommitted().getSeq()
                    + " guesses=" + state.getReceivedGuesses() + "/" + state.getRequiredGuesses()
                    + " settling=" + state.getSettling());
        }

        @Override public void close() {
            if (channel == null) return;
            channel.shutdownNow();
            try { channel.awaitTermination(2, TimeUnit.SECONDS); }
            catch (InterruptedException error) { Thread.currentThread().interrupt(); }
        }
    }
}
