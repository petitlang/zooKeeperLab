package fr.isep.storage;

import fr.isep.proto.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class CheckpointTest {
    @TempDir Path directory;

    private LogEntry entry(int seq, LogStatus status) {
        return LogEntry.newBuilder().setTag(ProposalTag.newBuilder().setEpoch(1).setSeq(seq))
                .setOp(Operation.WRITE).setValue(seq).setStatus(status).setRoundId(seq)
                .setClientId("a").setRequestId("r" + seq).build();
    }

    @Test void checkpointsReplayAcrossEpochsAndPreserveCommits() throws Exception {
        Path path = directory.resolve("log");
        List<LogEntry> history = List.of(entry(1, LogStatus.COMMITTED), entry(2, LogStatus.PROPOSED));
        try (LogStore store = new LogStore(path)) {
            store.installSnapshot(history, 2);
            assertThrows(IllegalArgumentException.class, () -> store.installSnapshot(List.of(), 3));
            assertThrows(IllegalArgumentException.class, () -> store.installSnapshot(
                    List.of(entry(1, LogStatus.PROPOSED)), 2));
            store.commit(history.get(1).getTag());
        }
        try (LogStore store = new LogStore(path)) {
            assertEquals(2, store.getCurrentEpoch());
            assertTrue(store.getEntries().stream().allMatch(e -> e.getStatus() == LogStatus.COMMITTED));
        }
    }

    @Test void tornCheckpointDoesNotReplacePreviousDurableHistory() throws Exception {
        Path path = directory.resolve("log");
        try (LogStore store = new LogStore(path)) { store.installSnapshot(List.of(entry(1, LogStatus.COMMITTED)), 1); }
        Files.writeString(path, "SNAPSHOT\tincomplete", StandardOpenOption.APPEND);
        try (LogStore store = new LogStore(path)) { assertEquals(1, store.getEntries().size()); }
        Files.writeString(path, "SNAPSHOT\tcorrupt\n", StandardOpenOption.APPEND);
        assertThrows(java.io.IOException.class, () -> new LogStore(path));
    }

    @Test void rejectsGapsConflictsAndCommitAfterPending() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> LogStore.validateSnapshot(List.of(entry(2, LogStatus.PROPOSED)), 1));
        assertThrows(IllegalArgumentException.class, () -> LogStore.validateSnapshot(
                List.of(entry(1, LogStatus.PROPOSED), entry(2, LogStatus.COMMITTED)), 1));
        try (LogStore store = new LogStore(directory.resolve("log"))) {
            store.installSnapshot(List.of(entry(1, LogStatus.PROPOSED)), 1);
            assertThrows(IllegalArgumentException.class, () -> store.installSnapshot(
                    List.of(entry(1, LogStatus.PROPOSED).toBuilder().setValue(100).build()), 2));
        }
    }
}
