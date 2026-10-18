package fr.isep;

import fr.isep.node.NodeConfig;
import fr.isep.node.NodeServer;
import fr.isep.storage.LogStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.channels.SocketChannel;
import java.net.InetSocketAddress;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

/** Check JVM exit, duplicate startup, occupied ports, and released file locks. */
class ProcessLifecycleTest {
    @TempDir Path directory;
    private final int basePort = 18600;

    @Test
    void duplicateStartupFailsCleanlyAndConcurrentCloseReleasesResources() throws Exception {
        Path evidence = Path.of("target", "process-evidence");
        Files.createDirectories(evidence);
        Path firstOutput = evidence.resolve("normal-shutdown.log");
        Process first = start(directory, firstOutput);
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (!Files.readString(firstOutput).contains("PROCESS_READY") && first.isAlive()
                    && System.nanoTime() < deadline) {
                Thread.sleep(100);
            }
            assertTrue(Files.readString(firstOutput).contains("PROCESS_READY"), Files.readString(firstOutput));

            // Same data directory must reject a second writer without disturbing the first JVM.
            Path duplicateOutput = evidence.resolve("duplicate-node.log");
            Process duplicate = start(directory, duplicateOutput);
            try {
                assertTrue(duplicate.waitFor(10, TimeUnit.SECONDS));
                assertNotEquals(0, duplicate.exitValue());
                assertTrue(Files.readString(duplicateOutput).contains("Log is already in use"));
            } finally {
                terminate(duplicate);
            }
            assertTrue(first.isAlive());

            // Different data directory, same TCP port: failed startup must also release its log.
            Path otherData = directory.resolve("other-data");
            Path conflictOutput = evidence.resolve("port-conflict.log");
            Process conflict = start(otherData, conflictOutput);
            try {
                assertTrue(conflict.waitFor(10, TimeUnit.SECONDS));
                assertNotEquals(0, conflict.exitValue());
                assertTrue(Files.readString(conflictOutput).contains("START_FAILED"));
            } finally {
                terminate(conflict);
            }
            try (LogStore recovered = new LogStore(new NodeConfig(1, basePort, otherData).getLogFile())) {
                assertTrue(recovered.getEntries().isEmpty());
            }

            first.getOutputStream().write("stop\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            first.getOutputStream().flush();
            assertTrue(first.waitFor(15, TimeUnit.SECONDS), "Shutdown hung: " + Files.readString(firstOutput));
            assertEquals(0, first.exitValue(), Files.readString(firstOutput));
            assertTrue(Files.readString(firstOutput).contains("PROCESS_CLOSED"));
            assertFalse(Files.readString(firstOutput).contains("Exception in thread"));
            try (LogStore recovered = new LogStore(new NodeConfig(1, basePort, directory).getLogFile())) {
                assertTrue(recovered.getEntries().isEmpty());
            }
            // Rebinding verifies the stopped JVM no longer owns its listening port.
            try (java.nio.channels.ServerSocketChannel socket = java.nio.channels.ServerSocketChannel.open()) {
                socket.bind(new InetSocketAddress("127.0.0.1", basePort + 1));
            }
            Files.writeString(evidence.resolve("result.txt"),
                    "PASS: no duplicate writer, port-conflict JVM exits, concurrent close exits, log locks and port released.\n");
        } finally {
            terminate(first);
        }
    }

    private Process start(Path data, Path output) throws Exception {
        return new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java.exe").toString(),
                "-Djdk.net.unixdomain.tmpdir=" + directory.resolve("nonexistent-socket-dir"), "-cp",
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                LifecycleProcess.class.getName(), data.toString(), Integer.toString(basePort))
                .redirectErrorStream(true).redirectOutput(output.toFile()).start();
    }

    private static void terminate(Process process) throws Exception {
        if (process.isAlive()) {
            process.destroyForcibly();
            assertTrue(process.waitFor(5, TimeUnit.SECONDS));
        }
    }

    public static class LifecycleProcess {
        public static void main(String[] args) throws Exception {
            try (NodeServer node = new NodeServer(new NodeConfig(1, Integer.parseInt(args[1]), Path.of(args[0])))) {
                Runtime.getRuntime().addShutdownHook(new Thread(node::close));
                node.start();
                System.out.println("PROCESS_READY");
                new Scanner(System.in).nextLine();
                // Two callers model the main thread and shutdown hook racing to close.
                Thread a = new Thread(node::close);
                Thread b = new Thread(node::close);
                a.start();
                b.start();
                a.join(10000);
                b.join(10000);
                if (a.isAlive() || b.isAlive()) {
                    System.err.println("CLOSE_HUNG");
                    Runtime.getRuntime().halt(1);
                }
                System.out.println("PROCESS_CLOSED");
            } catch (Exception e) {
                System.err.println("START_FAILED: " + e.getMessage());
                System.exit(1);
            }
        }
    }
}
