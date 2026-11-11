/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.ingestion;

import java.time.Duration;
import java.util.Objects;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>The complete, deliberately arithmetic rule set that turns a noisy stream of
 * {@link MotionAlert}s into clean occupancy signals. Four classical techniques, no inference:</p>
 *
 * <ol>
 *   <li><strong>Schmitt-trigger hysteresis.</strong> Two thresholds with a dead band between them.
 *       A table is judged occupied above {@code occupiedMotionRatio} and vacant below
 *       {@code vacantMotionRatio}; readings inside the band change nothing. A single threshold
 *       would chatter endlessly whenever a table's motion hovered around it.</li>
 *   <li><strong>N-of-N confirmation.</strong> A judgement must repeat for
 *       {@code confirmationFrames} consecutive qualifying alerts. This is what rejects a waiter
 *       walking past the camera.</li>
 *   <li><strong>Confidence floor.</strong> Alerts below {@code minConfidence} are discarded without
 *       disturbing accumulated evidence, so a momentarily blinded camera does not erase a
 *       half-formed judgement.</li>
 *   <li><strong>Emission debounce.</strong> At most one signal per table per
 *       {@code emitDebounce} window, bounding downstream write and broadcast amplification
 *       regardless of frame rate.</li>
 * </ol>
 *
 * @param occupiedMotionRatio lower bound of the "occupied" band, in {@code 0.0..1.0}
 * @param vacantMotionRatio   upper bound of the "vacant" band; must be below
 *                            {@code occupiedMotionRatio} so that a dead band genuinely exists
 * @param confirmationFrames  consecutive qualifying alerts required to confirm a judgement
 * @param minConfidence       minimum edge confidence an alert must carry to be considered
 * @param emitDebounce        minimum interval between two emitted signals for the same table
 * @param trackRetention      how long per-table evidence is retained after the last alert before
 *                            the tracking entry is evicted to bound memory
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public record MotionConditioningPolicy(
        double occupiedMotionRatio,
        double vacantMotionRatio,
        int confirmationFrames,
        double minConfidence,
        Duration emitDebounce,
        Duration trackRetention) {

    /** Canonical constructor enforcing that the policy describes a usable hysteresis band. */
    public MotionConditioningPolicy {
        Objects.requireNonNull(emitDebounce, "emitDebounce must not be null");
        Objects.requireNonNull(trackRetention, "trackRetention must not be null");
        if (occupiedMotionRatio < 0.0d || occupiedMotionRatio > 1.0d) {
            throw new IllegalArgumentException(
                    "occupiedMotionRatio must be within [0.0, 1.0] but was " + occupiedMotionRatio);
        }
        if (vacantMotionRatio < 0.0d || vacantMotionRatio > 1.0d) {
            throw new IllegalArgumentException(
                    "vacantMotionRatio must be within [0.0, 1.0] but was " + vacantMotionRatio);
        }
        if (vacantMotionRatio >= occupiedMotionRatio) {
            throw new IllegalArgumentException(
                    "vacantMotionRatio (%s) must be strictly below occupiedMotionRatio (%s) so that a hysteresis dead band exists"
                            .formatted(vacantMotionRatio, occupiedMotionRatio));
        }
        if (confirmationFrames < 1) {
            throw new IllegalArgumentException(
                    "confirmationFrames must be at least 1 but was " + confirmationFrames);
        }
        if (minConfidence < 0.0d || minConfidence > 1.0d) {
            throw new IllegalArgumentException(
                    "minConfidence must be within [0.0, 1.0] but was " + minConfidence);
        }
        if (emitDebounce.isNegative()) {
            throw new IllegalArgumentException("emitDebounce must not be negative");
        }
        if (trackRetention.isZero() || trackRetention.isNegative()) {
            throw new IllegalArgumentException("trackRetention must be strictly positive");
        }
    }

    /**
     * A sensible production starting point for an indoor dining room at 1–5 alerts per second.
     *
     * @return a policy with a 4%–18% dead band, 3-frame confirmation and a 20 second debounce
     */
    public static MotionConditioningPolicy defaults() {
        return new MotionConditioningPolicy(
                0.18d, 0.04d, 3, 0.45d, Duration.ofSeconds(20), Duration.ofMinutes(30));
    }
}
