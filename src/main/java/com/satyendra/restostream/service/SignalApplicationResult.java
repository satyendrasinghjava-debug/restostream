/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.service;

import com.satyendra.restostream.domain.TableSnapshot;
import com.satyendra.restostream.engine.TransitionDecision;

import java.util.Objects;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>The outcome of driving one signal all the way through the pipeline: what the engine decided,
 * the table state that is now authoritative in Redis, and how many compare-and-set attempts it
 * took.</p>
 *
 * <p>{@code attempts} is not diagnostic padding — it is the contention signal for this aggregate.
 * A rising mean means several transports are fighting over the same tables, which is the cue to
 * reconsider ingestion fan-in long before it shows up as user-visible latency.</p>
 *
 * @param decision the engine's decision
 * @param snapshot the authoritative table state after the decision was executed
 * @param attempts number of compare-and-set attempts made; {@code 0} when no write was needed
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public record SignalApplicationResult(TransitionDecision decision,
                                      TableSnapshot snapshot,
                                      int attempts) {

    /** Canonical constructor enforcing non-nullity and a sane attempt count. */
    public SignalApplicationResult {
        Objects.requireNonNull(decision, "decision must not be null");
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        if (attempts < 0) {
            throw new IllegalArgumentException("attempts must not be negative but was " + attempts);
        }
    }

    /**
     * @return {@code true} when the state machine actually advanced
     */
    public boolean applied() {
        return decision.isApplied();
    }
}
