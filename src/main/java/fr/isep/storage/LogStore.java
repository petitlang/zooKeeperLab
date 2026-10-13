package fr.isep.storage;

import fr.isep.proto.LogEntry;
import fr.isep.proto.LogStatus;
import fr.isep.proto.Operation;
import fr.isep.proto.ProposalTag;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/** Persistent proposal records, commit updates, and epoch metadata. */
public final class LogStore implements AutoCloseable {
    private static final Comparator<ProposalTag> TAG_ORDER = Comparator
            .comparingInt(ProposalTag::getEpoch).thenComparingInt(ProposalTag::getSeq);

    private final Path path;
    private final FileChannel channel;
    private final FileLock lock;
    private final NavigableMap<ProposalTag, LogEntry> entries = new TreeMap<>(TAG_ORDER);
    private int currentEpoch;
    private boolean healthy = true;

    public LogStore(Path path) throws IOException {
        this.path = Objects.requireNonNull(path, "path").toAbsolutePath().normalize();
        Files.createDirectories(this.path.getParent());
        channel = FileChannel.open(this.path, StandardOpenOption.CREATE,
                StandardOpenOption.READ, StandardOpenOption.WRITE);
        try {
            lock = channel.tryLock();
            if (lock == null) {
                throw new IOException("Log is already in use: " + this.path);
            }
            replay();
        } catch (OverlappingFileLockException e) {
            channel.close();
            throw new IOException("Log is already in use: " + this.path, e);
        } catch (IOException | RuntimeException e) {
            try {
                channel.close();
            } catch (IOException closeError) {
                e.addSuppressed(closeError);
            }
            throw e;
        }
    }

    /** Persist a new epoch even when that epoch has no proposal yet. */
    public synchronized void recordEpoch(int epoch) throws IOException {
        ensureOpen();
        if (epoch < currentEpoch) {
            throw new IllegalArgumentException("Epoch cannot decrease.");
        }
        if (epoch != currentEpoch) {
            persist("EPOCH\t" + epoch + "\n");
            currentEpoch = epoch;
        }
    }

    /** Return only after the complete PROPOSED record has been forced to disk. */
    public synchronized LogEntry appendProposal(LogEntry proposal) throws IOException {
        ensureOpen();
        validateEntry(proposal);
        if (proposal.getStatus() != LogStatus.PROPOSED) {
            throw new IllegalArgumentException("Expected a PROPOSED entry.");
        }
        LogEntry existing = entries.get(proposal.getTag());
        if (existing != null) {
            requireSameProposal(existing, proposal);
            return existing; // A retry must not duplicate or downgrade a committed entry.
        }
        validateNextProposal(proposal);
        persist(encodeEntry(proposal));
        entries.put(proposal.getTag(), proposal);
        currentEpoch = Math.max(currentEpoch, proposal.getTag().getEpoch());
        return proposal;
    }

    /** Commit one entry in log order; repeating a commit is harmless. */
    public synchronized LogEntry commit(ProposalTag tag) throws IOException {
        ensureOpen();
        LogEntry existing = requireEntry(tag);
        if (existing.getStatus() == LogStatus.COMMITTED) {
            return existing;
        }
        requireNextCommit(tag);
        LogEntry committed = existing.toBuilder().setStatus(LogStatus.COMMITTED).build();
        persist(encodeEntry(committed));
        entries.put(tag, committed);
        return committed;
    }

    public synchronized int getCurrentEpoch() {
        ensureOpen();
        return currentEpoch;
    }

    public synchronized List<LogEntry> getEntries() {
        ensureOpen();
        return List.copyOf(entries.values());
    }

    public synchronized Optional<LogEntry> find(ProposalTag tag) {
        ensureOpen();
        return Optional.ofNullable(entries.get(Objects.requireNonNull(tag, "tag")));
    }

    public synchronized Optional<LogEntry> getLatestEntry() {
        ensureOpen();
        return entries.isEmpty() ? Optional.empty() : Optional.of(entries.lastEntry().getValue());
    }

    public synchronized Optional<LogEntry> getLatestCommittedEntry() {
        ensureOpen();
        return entries.descendingMap().values().stream()
                .filter(entry -> entry.getStatus() == LogStatus.COMMITTED).findFirst();
    }

    private void validateNextProposal(LogEntry entry) {
        ProposalTag tag = entry.getTag();
        if (tag.getEpoch() < currentEpoch) {
            throw new IllegalArgumentException("Proposal belongs to an old epoch.");
        }
        LogEntry latest = entries.isEmpty() ? null : entries.lastEntry().getValue();
        int expectedSeq = latest != null && latest.getTag().getEpoch() == tag.getEpoch()
                ? Math.addExact(latest.getTag().getSeq(), 1) : 1;
        long expectedRound = latest == null ? 1 : Math.addExact(latest.getRoundId(), 1);
        if ((latest != null && TAG_ORDER.compare(tag, latest.getTag()) <= 0)
                || tag.getSeq() != expectedSeq || entry.getRoundId() != expectedRound) {
            throw new IllegalArgumentException("Proposal must extend the ordered log without gaps.");
        }
    }

    private LogEntry requireEntry(ProposalTag tag) {
        LogEntry entry = entries.get(Objects.requireNonNull(tag, "tag"));
        if (entry == null) {
            throw new IllegalArgumentException("Unknown proposal: " + tag.getEpoch() + ":" + tag.getSeq());
        }
        return entry;
    }

    private void requireNextCommit(ProposalTag tag) {
        ProposalTag next = entries.values().stream()
                .filter(entry -> entry.getStatus() == LogStatus.PROPOSED)
                .map(LogEntry::getTag).findFirst().orElseThrow();
        if (TAG_ORDER.compare(next, tag) != 0) {
            throw new IllegalArgumentException("Earlier proposals must be committed first.");
        }
    }

    private static void validateEntry(LogEntry entry) {
        Objects.requireNonNull(entry, "entry");
        if (!entry.hasTag() || entry.getTag().getEpoch() <= 0 || entry.getTag().getSeq() <= 0
                || (entry.getOp() != Operation.WRITE && entry.getOp() != Operation.GUESS)
                || entry.getValue() < 0 || entry.getValue() > 100 || entry.getRoundId() <= 0
                || (entry.getStatus() != LogStatus.PROPOSED && entry.getStatus() != LogStatus.COMMITTED)) {
            throw new IllegalArgumentException("Invalid log entry.");
        }
    }

    private static void requireSameProposal(LogEntry existing, LogEntry incoming) {
        if (!existing.toBuilder().setStatus(LogStatus.PROPOSED).build()
                .equals(incoming.toBuilder().setStatus(LogStatus.PROPOSED).build())) {
            throw new IllegalArgumentException("Conflicting data for the same proposal tag.");
        }
    }

    private void persist(String record) throws IOException {
        try {
            ByteBuffer bytes = ByteBuffer.wrap(record.getBytes(StandardCharsets.UTF_8));
            channel.position(channel.size());
            while (bytes.hasRemaining()) {
                channel.write(bytes);
            }
            channel.force(true);
        } catch (IOException e) {
            // A partial write is uncertain. Stop using this instance until reopened.
            healthy = false;
            throw e;
        }
    }

    private void replay() throws IOException {
        if (channel.size() > Integer.MAX_VALUE) {
            throw new IOException("Journal is too large for this lab's in-memory log.");
        }
        ByteBuffer buffer = ByteBuffer.allocate((int) channel.size());
        channel.position(0);
        while (buffer.hasRemaining()) {
            if (channel.read(buffer) < 0) {
                throw new IOException("Unexpected end of journal.");
            }
        }
        byte[] bytes = buffer.array();
        int completeLength = bytes.length;
        while (completeLength > 0 && bytes[completeLength - 1] != '\n') {
            completeLength--;
        }
        // A crash can leave an unfinished final append, which was never acknowledged.
        if (completeLength != bytes.length) {
            channel.truncate(completeLength);
            channel.force(true);
        }
        String content = new String(bytes, 0, completeLength, StandardCharsets.UTF_8);
        String[] lines = content.split("\n", -1);
        // Ignore only the empty segment after the final newline, not blank records.
        for (int index = 0; index < lines.length - 1; index++) {
            try {
                replayLine(lines[index]);
            } catch (IllegalArgumentException | IllegalStateException | ArithmeticException e) {
                throw new IOException("Invalid journal record at " + path + ":" + (index + 1), e);
            }
        }
    }

    private void replayLine(String line) {
        String[] fields = line.split("\t", -1);
        if (fields.length == 2 && "EPOCH".equals(fields[0])) {
            int epoch = Integer.parseInt(fields[1]);
            if (epoch < currentEpoch) {
                throw new IllegalArgumentException("Epoch regression in journal.");
            }
            currentEpoch = epoch;
            return;
        }
        if (fields.length != 9 || !"ENTRY".equals(fields[0])) {
            throw new IllegalArgumentException("Unknown journal record.");
        }
        LogEntry entry = LogEntry.newBuilder()
                .setTag(ProposalTag.newBuilder().setEpoch(Integer.parseInt(fields[1]))
                        .setSeq(Integer.parseInt(fields[2])))
                .setOp(Operation.valueOf(fields[3])).setValue(Integer.parseInt(fields[4]))
                .setStatus(LogStatus.valueOf(fields[5]))
                .setClientId(decodeId(fields[6])).setRequestId(decodeId(fields[7]))
                .setRoundId(Long.parseLong(fields[8])).build();
        validateEntry(entry);
        LogEntry existing = entries.get(entry.getTag());
        if (entry.getStatus() == LogStatus.PROPOSED) {
            if (existing == null) {
                validateNextProposal(entry);
                entries.put(entry.getTag(), entry);
                currentEpoch = Math.max(currentEpoch, entry.getTag().getEpoch());
            } else {
                requireSameProposal(existing, entry);
            }
        } else {
            existing = requireEntry(entry.getTag());
            requireSameProposal(existing, entry);
            if (existing.getStatus() != LogStatus.COMMITTED) {
                requireNextCommit(entry.getTag());
                entries.put(entry.getTag(), entry);
            }
        }
    }

    private static String encodeEntry(LogEntry entry) {
        return String.join("\t", "ENTRY", Integer.toString(entry.getTag().getEpoch()),
                Integer.toString(entry.getTag().getSeq()), entry.getOp().name(),
                Integer.toString(entry.getValue()), entry.getStatus().name(),
                encodeId(entry.getClientId()), encodeId(entry.getRequestId()),
                Long.toString(entry.getRoundId())) + "\n";
    }

    private static String encodeId(String id) {
        return Base64.getUrlEncoder().encodeToString(id.getBytes(StandardCharsets.UTF_8));
    }

    private static String decodeId(String encoded) {
        return new String(Base64.getUrlDecoder().decode(encoded), StandardCharsets.UTF_8);
    }

    private void ensureOpen() {
        if (!channel.isOpen() || !lock.isValid() || !healthy) {
            throw new IllegalStateException("Log store is closed or unavailable.");
        }
    }

    @Override
    public synchronized void close() throws IOException {
        channel.close(); // Closing the channel also releases the exclusive file lock.
    }
}
