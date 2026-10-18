package fr.isep.coordination;

import fr.isep.proto.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class RecoveryTest {
    private LogSnapshot snapshot(int node, LogStatus status) {
        return LogSnapshot.newBuilder().setFromNodeId(node).setCurrentEpoch(1)
                .addEntries(LogEntry.newBuilder().setTag(ProposalTag.newBuilder().setEpoch(1).setSeq(1))
                        .setOp(Operation.WRITE).setValue(50).setStatus(status)
                        .setClientId("a").setRequestId("r").setRoundId(1)).build();
    }

    @Test void selectionIncludesProposalsAndBreaksTiesByLowestId() {
        LogSnapshot a = snapshot(1, LogStatus.PROPOSED), b = snapshot(2, LogStatus.COMMITTED);
        assertTrue(RecoveryManager.compareCandidates(a, b) > 0);
        assertTrue(RecoveryManager.compareCandidates(b, LogSnapshot.newBuilder().setFromNodeId(1).build()) > 0);
    }

    @Test void mergesCommitKnowledgeButNeverInventsAnUnobservedCommit() {
        LogSnapshot a = snapshot(1, LogStatus.PROPOSED), b = snapshot(2, LogStatus.COMMITTED);
        assertEquals(LogStatus.COMMITTED, RecoveryManager.reconcile(a, List.of(a, b)).get(0).getStatus());
        assertEquals(LogStatus.PROPOSED, RecoveryManager.reconcile(a, List.of(a)).get(0).getStatus());
        LogSnapshot conflict = b.toBuilder().setEntries(0, b.getEntries(0).toBuilder().setValue(20)).build();
        assertThrows(IllegalArgumentException.class, () -> RecoveryManager.reconcile(a, List.of(a, conflict)));
        assertThrows(IllegalArgumentException.class, () -> RecoveryManager.reconcile(
                LogSnapshot.newBuilder().setFromNodeId(3).build(), List.of(a)));
    }
}
