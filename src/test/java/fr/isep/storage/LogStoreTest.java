package fr.isep.storage;

import fr.isep.proto.LogEntry;
import fr.isep.proto.LogStatus;
import fr.isep.proto.Operation;
import fr.isep.proto.ProposalTag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class LogStoreTest {
    @TempDir
    Path directory;

    private Path logFile() {
        return directory.resolve("entries.log");
    }

    private static LogEntry proposal(int epoch, int seq, long round, int value) {
        return LogEntry.newBuilder().setTag(ProposalTag.newBuilder().setEpoch(epoch).setSeq(seq))
                .setOp(Operation.WRITE).setValue(value).setStatus(LogStatus.PROPOSED)
                .setClientId("client\t中文\n").setRequestId("request-" + round).setRoundId(round).build();
    }

    @Test
    void restartRestoresCommittedPrefixPendingTailAndEpochWithoutProposals() throws Exception {
        LogEntry first = proposal(1, 1, 1, 0);
        LogEntry pending = proposal(1, 2, 2, 62);
        try (LogStore log = new LogStore(logFile())) {
            log.appendProposal(first);
            log.commit(first.getTag());
            log.appendProposal(pending);
            log.recordEpoch(2);
        }
        try (LogStore restored = new LogStore(logFile())) {
            assertEquals(2, restored.getCurrentEpoch());
            assertEquals(2, restored.getEntries().size());
            assertEquals(0, restored.getLatestCommittedEntry().orElseThrow().getValue());
            assertEquals(first.getClientId(), restored.getEntries().get(0).getClientId());
            assertEquals(pending, restored.getLatestEntry().orElseThrow());
        }
    }

    @Test
    void retriesDoNotDuplicateEntriesOrDowngradeCommittedData() throws Exception {
        LogEntry proposal = proposal(1, 1, 1, 50);
        try (LogStore log = new LogStore(logFile())) {
            log.appendProposal(proposal);
            long proposedSize = Files.size(logFile());
            log.appendProposal(proposal);
            assertEquals(proposedSize, Files.size(logFile()));
            log.commit(proposal.getTag());
            long committedSize = Files.size(logFile());
            assertEquals(LogStatus.COMMITTED, log.appendProposal(proposal).getStatus());
            log.commit(proposal.getTag());
            assertEquals(committedSize, Files.size(logFile()));
            assertEquals(1, log.getEntries().size());
        }
    }

    @Test
    void rejectsConflictsUnknownCommitsAndOutOfOrderCommits() throws Exception {
        LogEntry first = proposal(1, 1, 1, 50);
        LogEntry second = proposal(1, 2, 2, 62);
        try (LogStore log = new LogStore(logFile())) {
            log.appendProposal(first);
            log.appendProposal(second);
            assertThrows(IllegalArgumentException.class,
                    () -> log.appendProposal(first.toBuilder().setValue(10).build()));
            assertThrows(IllegalArgumentException.class, () -> log.commit(second.getTag()));
            assertThrows(IllegalArgumentException.class,
                    () -> log.commit(ProposalTag.newBuilder().setEpoch(9).setSeq(9).build()));
            assertTrue(log.getLatestCommittedEntry().isEmpty());
            log.commit(first.getTag());
            log.commit(second.getTag());
            assertEquals(62, log.getLatestCommittedEntry().orElseThrow().getValue());
        }
    }

    @Test
    void rejectsGapsStaleEpochsAndInvalidValuesWithoutAppending() throws Exception {
        try (LogStore log = new LogStore(logFile())) {
            assertThrows(IllegalArgumentException.class, () -> log.appendProposal(proposal(1, 2, 1, 50)));
            assertThrows(IllegalArgumentException.class, () -> log.appendProposal(proposal(1, 1, 1, 101)));
            assertThrows(IllegalArgumentException.class, () -> log.appendProposal(proposal(1, 1, 1, -1)));
            log.recordEpoch(2);
            assertThrows(IllegalArgumentException.class, () -> log.recordEpoch(1));
            assertThrows(IllegalArgumentException.class, () -> log.appendProposal(proposal(1, 1, 1, 50)));
            assertTrue(log.getEntries().isEmpty());
        }
    }

    @Test
    void truncatesOnlyAnUnfinishedFinalRecordAndCanAppendAgain() throws Exception {
        LogEntry first = proposal(1, 1, 1, 50);
        try (LogStore log = new LogStore(logFile())) {
            log.appendProposal(first);
            log.commit(first.getTag());
        }
        long validSize = Files.size(logFile());
        Files.writeString(logFile(), "ENTRY\t1\t2\tWRITE", StandardOpenOption.APPEND);
        try (LogStore restored = new LogStore(logFile())) {
            assertEquals(validSize, Files.size(logFile()));
            assertEquals(50, restored.getLatestCommittedEntry().orElseThrow().getValue());
            restored.appendProposal(proposal(1, 2, 2, 62));
        }
        try (LogStore restoredAgain = new LogStore(logFile())) {
            assertEquals(2, restoredAgain.getEntries().size());
        }
    }

    @Test
    void completeCorruptRecordStopsRecoveryAndReleasesLock() throws Exception {
        for (String corrupt : List.of("BROKEN\n", "\n", "EPOCH\t1\n\n", "EPOCH\t-1\n")) {
            Files.writeString(logFile(), corrupt);
            assertThrows(IOException.class, () -> new LogStore(logFile()));
            assertEquals(corrupt, Files.readString(logFile()));
        }
        Files.writeString(logFile(), "");
        try (LogStore repaired = new LogStore(logFile())) {
            assertTrue(repaired.getEntries().isEmpty());
        }
    }

    @Test
    void exclusiveLockPreventsTwoWritersAndCloseReleasesIt() throws Exception {
        try (LogStore first = new LogStore(logFile())) {
            assertThrows(IOException.class, () -> new LogStore(logFile()));
            first.recordEpoch(1);
        }
        try (LogStore second = new LogStore(logFile())) {
            assertEquals(1, second.getCurrentEpoch());
        }
    }

    @Test
    void forcedProcessExitPreservesAcknowledgableRecordsWithoutClose() throws Exception {
        Path output = directory.resolve("crash-writer.log");
        Process child = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java.exe").toString(), "-cp",
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")),
                CrashWriter.class.getName(), logFile().toString())
                .redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            assertTrue(child.waitFor(10, TimeUnit.SECONDS), "Crash writer timed out");
            assertEquals(0, child.exitValue(), Files.readString(output));
        } finally {
            if (child.isAlive()) {
                child.destroyForcibly();
                child.waitFor(5, TimeUnit.SECONDS);
            }
        }
        try (LogStore restored = new LogStore(logFile())) {
            assertEquals(2, restored.getCurrentEpoch());
            assertEquals(50, restored.getLatestCommittedEntry().orElseThrow().getValue());
            assertEquals(LogStatus.PROPOSED, restored.getLatestEntry().orElseThrow().getStatus());
        }
    }

    public static class CrashWriter {
        public static void main(String[] args) throws Exception {
            LogStore log = new LogStore(Path.of(args[0]));
            LogEntry first = proposal(1, 1, 1, 50);
            log.appendProposal(first);
            log.commit(first.getTag());
            log.appendProposal(proposal(1, 2, 2, 62));
            log.recordEpoch(2);
            Runtime.getRuntime().halt(0); // No close or JVM shutdown hooks.
        }
    }
}
