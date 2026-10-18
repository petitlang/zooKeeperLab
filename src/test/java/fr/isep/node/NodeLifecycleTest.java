package fr.isep.node;

import fr.isep.storage.LogStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class NodeLifecycleTest {
    @TempDir Path directory;

    @Test void duplicatePortFailureAndConcurrentCloseReleaseResources() throws Exception {
        int port;
        try (ServerSocket reservation = new ServerSocket(0)) { port = reservation.getLocalPort(); }
        NodeConfig firstConfig = new NodeConfig(1, port - 1, directory.resolve("first"));
        NodeConfig otherConfig = new NodeConfig(1, port - 1, directory.resolve("other"));
        NodeServer first = new NodeServer(firstConfig, false);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            first.start();
            assertThrows(IOException.class, () -> new NodeServer(firstConfig, false));
            try (NodeServer conflict = new NodeServer(otherConfig, false)) {
                assertThrows(IOException.class, conflict::start);
            }
            try (LogStore released = new LogStore(otherConfig.getLogFile())) { assertEquals(0, released.getCurrentEpoch()); }
            Future<?> a = workers.submit(first::close), b = workers.submit(first::close);
            a.get(15, TimeUnit.SECONDS); b.get(15, TimeUnit.SECONDS);
            try (NodeServer restarted = new NodeServer(firstConfig, false)) { restarted.start(); }
            try (ServerSocket released = new ServerSocket(port)) { assertEquals(port, released.getLocalPort()); }
        } finally {
            first.close();
            workers.shutdownNow();
        }
    }
}
