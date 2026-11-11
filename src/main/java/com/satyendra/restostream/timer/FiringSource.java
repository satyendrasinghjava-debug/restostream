/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.timer;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Which of the two independent detection paths noticed that an SLA deadline had elapsed.
 * Recorded on every firing and surfaced in metrics, because the ratio between the two is the single
 * most useful operational signal about the timer subsystem's health: a sweep-dominated ratio means
 * keyspace notifications are not reaching the service and breach latency has silently degraded from
 * milliseconds to the sweep interval.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public enum FiringSource {

    /** A Redis key-expiry pub/sub event. Sub-second, but fire-and-forget and therefore lossy. */
    KEYSPACE_EXPIRY,

    /** The reconciliation sweep over the deadline index. Bounded latency, but never lossy. */
    RECONCILIATION_SWEEP
}
