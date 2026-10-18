package fr.isep.client;

import fr.isep.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

// 与以前的 GrpcClient 一样使用 blocking stub；菜单沿用 SocketClient 的 Scanner 循环。
public final class ClientApp implements AutoCloseable {
    private final ManagedChannel channel;
    private final GameServiceGrpc.GameServiceBlockingStub stub;
    private final String clientId;

    public ClientApp(int port, String clientId) {
        this.clientId = clientId;
        channel = ManagedChannelBuilder.forAddress("127.0.0.1", port).usePlaintext().build();
        stub = GameServiceGrpc.newBlockingStub(channel).withDeadlineAfter(6, TimeUnit.SECONDS);
    }

    public void execute(String command) {
        String[] parts = command.trim().split("\\s+");
        if (parts.length == 1 && parts[0].equalsIgnoreCase("read")) {
            System.out.println(format(stub.read(ReadRequest.getDefaultInstance())));
            return;
        }
        if (parts.length != 2 || (!parts[0].equalsIgnoreCase("write") && !parts[0].equalsIgnoreCase("guess"))) {
            throw new IllegalArgumentException("Commands: read | write <0..100> | guess <0..100> | exit");
        }
        int value = Integer.parseInt(parts[1]);
        if (value < 0 || value > 100) {
            throw new IllegalArgumentException("Value must be between 0 and 100.");
        }
        String requestId = UUID.randomUUID().toString();
        OperationResponse response;
        if (parts[0].equalsIgnoreCase("write")) {
            WriteRequest request = WriteRequest.newBuilder().setClientId(clientId)
                    .setRequestId(requestId).setValue(value).build();
            response = stub.write(request);
        } else {
            GameState current = stub.read(ReadRequest.getDefaultInstance());
            GuessRequest request = GuessRequest.newBuilder().setClientId(clientId)
                    .setRequestId(requestId).setValue(value).setRoundId(current.getRoundId()).build();
            response = stub.guess(request);
        }
        System.out.println(response.getStatus() + " " + response.getMessage());
        System.out.println("request=" + requestId);
        System.out.println(format(response.getState()));
    }

    public static String format(GameState state) {
        return "node=" + state.getNodeId() + " role=" + state.getRole()
                + " leader=" + (state.hasLeaderId() ? state.getLeaderId() : "unknown")
                + " ready=" + state.getReady()
                + " target=" + (state.hasTarget() ? state.getTarget() : "unset")
                + " round=" + state.getRoundId()
                + " tag=(" + state.getLastCommitted().getEpoch() + ","
                + state.getLastCommitted().getSeq() + ")";
    }

    public static void main(String[] args) {
        int nodeId = 1;
        int basePort = 5000;
        String clientId = "client-" + UUID.randomUUID().toString().substring(0, 8);
        String command = "";
        try {
            int i = 0;
            while (i < args.length && args[i].startsWith("--")) {
                String option = args[i++];
                if (option.equals("--help")) {
                    usage();
                    return;
                }
                if (i >= args.length) {
                    throw new IllegalArgumentException("Missing value for " + option);
                }
                String value = args[i++];
                if (option.equals("--node")) {
                    nodeId = Integer.parseInt(value);
                } else if (option.equals("--base-port")) {
                    basePort = Integer.parseInt(value);
                } else if (option.equals("--client")) {
                    clientId = value;
                } else {
                    throw new IllegalArgumentException("Unknown option: " + option);
                }
            }
            if (nodeId < 1 || nodeId > 5 || basePort < 0 || basePort > 65530 || clientId.isBlank()) {
                throw new IllegalArgumentException("Invalid node, base port or client ID.");
            }
            if (i < args.length) {
                command = String.join(" ", java.util.Arrays.copyOfRange(args, i, args.length));
            }
            try (ClientApp client = new ClientApp(basePort + nodeId, clientId)) {
                if (!command.isEmpty()) {
                    client.execute(command);
                    return;
                }
                System.out.println("Client " + clientId + " connected to node " + nodeId);
                System.out.println("Commands: read | write <0..100> | guess <0..100> | exit");
                Scanner scanner = new Scanner(System.in);
                while (true) {
                    System.out.print("You: ");
                    if (!scanner.hasNextLine()) {
                        break;
                    }
                    String line = scanner.nextLine().trim();
                    if (line.equalsIgnoreCase("exit")) {
                        break;
                    }
                    try {
                        client.execute(line);
                    } catch (RuntimeException e) {
                        System.out.println("Error: " + e.getMessage());
                    }
                }
            }
        } catch (RuntimeException e) {
            System.err.println("Error: " + e.getMessage());
            usage();
            System.exit(2);
        }
    }

    private static void usage() {
        System.out.println("ClientApp [--node 1..5] [--client name] [--base-port 5000] [read|write N|guess N]");
    }

    @Override
    public void close() {
        channel.shutdownNow();
        try {
            channel.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
