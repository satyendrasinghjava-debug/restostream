/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.domain;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Provenance of an {@link OccupancySignal}. Recording the transport that produced a signal is
 * what makes the ingestion layer genuinely pluggable: audit trails, trust weighting and per-source
 * rate limiting all key off this value, and a new transport can be introduced without touching the
 * state engine.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public enum SignalSourceKind {

    /** A human tapped a tile on the dashboard or a hand-held device. Highest trust. */
    MANUAL_TAP(100),

    /** A waiter's mobile camera produced an occupancy judgement at the table side. */
    MOBILE_CAMERA(80),

    /**
     * A CCTV feed raised a motion/pixel-delta alert. Deliberately lightweight: a threshold with
     * hysteresis, never a machine-learning model.
     */
    CCTV_MOTION(60),

    /** The point-of-sale system opened or closed a ticket against the table. */
    POS_TICKET(90),

    /** A synthetic source used by load tests and demonstrations. Lowest trust. */
    SIMULATOR(10);

    private final int trustWeight;

    SignalSourceKind(int trustWeight) {
        this.trustWeight = trustWeight;
    }

    /**
     * Relative confidence that a signal from this transport reflects physical reality, on a
     * {@code 0..100} scale. Exposed so that conflict-resolution policies can be layered on later
     * without reshaping the ingestion contract.
     *
     * @return the trust weight of this transport
     */
    public int trustWeight() {
        return trustWeight;
    }
}
