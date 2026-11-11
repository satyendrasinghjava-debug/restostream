/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.timer;

import com.satyendra.restostream.domain.SlaKind;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Contract for the cluster-wide SLA timer subsystem. "Distributed" here means three concrete
 * guarantees, each of which the Redis implementation earns with a specific mechanism:</p>
 *
 * <ol>
 *   <li><strong>Survives instance loss.</strong> Timers live in Redis, not in a JVM scheduler. Kill
 *       the instance that armed a timer and the deadline still fires — on whichever instance is
 *       alive at the time.</li>
 *   <li><strong>Fires at most once, cluster-wide.</strong> {@link #claim} is an atomic
 *       read-remove in a single Lua script, so when three instances all observe the same expiry
 *       exactly one of them is told to alert.</li>
 *   <li><strong>Never silently loses a deadline.</strong> Expiry is detected twice over — by
 *       key-expiry notification for latency and by a sorted-set sweep for durability. Notifications
 *       may be dropped; the sweep cannot be, because the deadline index is persistent state rather
 *       than a message.</li>
 * </ol>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public interface DistributedTimerService {

    /**
     * Arms, or re-arms, the SLA timer for one table. Any timer previously armed for that table is
     * replaced atomically.
     *
     * @param tableId  the table; must not be {@code null}
     * @param slaKind  the SLA class being armed; must not be {@code null}
     * @param ttl      strictly-positive lifetime; the Redis key expires after this
     * @param deadline the absolute breach instant recorded in the deadline index
     * @return a {@link Mono} completing when the timer is armed
     */
    Mono<Void> arm(String tableId, SlaKind slaKind, Duration ttl, Instant deadline);

    /**
     * Cancels any SLA timer armed for one table. Idempotent: cancelling an unarmed table succeeds.
     *
     * @param tableId the table; must not be {@code null}
     * @return {@code true} when a timer was actually armed and has now been removed
     */
    Mono<Boolean> cancel(String tableId);

    /**
     * Atomically claims the elapsed deadline for one table.
     *
     * <p>Returns the claimed SLA class to exactly one caller cluster-wide, and an empty
     * {@link Mono} to every other caller — whether they lost the race, the deadline is not yet due,
     * or no timer is armed at all.</p>
     *
     * @param tableId the table whose deadline is believed to have elapsed; must not be {@code null}
     * @param now     the reference instant used to confirm the deadline is genuinely due
     * @return the claimed SLA class, or empty when this caller must not alert
     */
    Mono<SlaKind> claim(String tableId, Instant now);

    /**
     * Lists tables whose armed deadline is at or before {@code now}, without claiming them.
     *
     * @param now   the reference instant; must not be {@code null}
     * @param limit maximum number of identifiers to return; must be positive
     * @return overdue table identifiers, oldest deadline first
     */
    Flux<String> findOverdue(Instant now, int limit);

    /**
     * Reads the armed deadline for one table, for diagnostics and for the live countdown.
     *
     * @param tableId the table; must not be {@code null}
     * @return the armed deadline, or empty when no timer is armed
     */
    Mono<Instant> deadlineOf(String tableId);

    /**
     * @return the number of SLA timers currently armed across the cluster
     */
    Mono<Long> armedCount();
}
