/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.ingestion;

import com.satyendra.restostream.domain.SignalType;

import java.util.Objects;
import java.util.Optional;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>What the motion conditioner decided about one {@link MotionAlert}. Returned to the posting
 * camera so that an edge device can tell the difference between "accepted, still gathering
 * evidence" and "rejected, your confidence is too low" — and asserted directly in unit tests, which
 * is why the conditioner reports a disposition instead of silently swallowing alerts.</p>
 *
 * @param alertId        the alert this outcome concerns
 * @param tableId        the table the alert concerned
 * @param disposition    the conditioner's verdict
 * @param emittedSignal  the signal pushed into the pipeline, or {@code null} when none was
 * @param occupiedStreak consecutive qualifying "occupied" alerts after processing
 * @param vacantStreak   consecutive qualifying "vacant" alerts after processing
 * @param narrative      operator-facing explanation of the verdict
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public record MotionIngestOutcome(
        String alertId,
        String tableId,
        Disposition disposition,
        SignalType emittedSignal,
        int occupiedStreak,
        int vacantStreak,
        String narrative) {

    /** Canonical constructor enforcing non-nullity of the mandatory components. */
    public MotionIngestOutcome {
        Objects.requireNonNull(alertId, "alertId must not be null");
        Objects.requireNonNull(tableId, "tableId must not be null");
        Objects.requireNonNull(disposition, "disposition must not be null");
        Objects.requireNonNull(narrative, "narrative must not be null");
    }

    /**
     * @return the emitted signal, or {@link Optional#empty()} when the alert produced none
     */
    public Optional<SignalType> emitted() {
        return Optional.ofNullable(emittedSignal);
    }

    /**
     * The verdicts the motion conditioner can reach.
     *
     * @author Satyendra Singh
     */
    public enum Disposition {

        /** Evidence was sufficient; a signal was pushed into the ingestion pipeline. */
        EMITTED,

        /** The alert qualified but the confirmation streak is not yet long enough. */
        AWAITING_CONFIRMATION,

        /** The reading fell inside the hysteresis dead band; accumulated evidence was preserved. */
        AMBIGUOUS,

        /** Edge confidence was below the configured floor; the alert was discarded. */
        LOW_CONFIDENCE,

        /** Evidence was sufficient but the per-table emission debounce window is still open. */
        DEBOUNCED,

        /** Evidence confirmed a judgement this conditioner has already emitted. */
        DUPLICATE_ASSERTION
    }
}
