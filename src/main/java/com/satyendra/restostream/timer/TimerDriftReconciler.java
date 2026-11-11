/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.timer;

import com.satyendra.restostream.config.RestoStreamProperties;
import com.satyendra.restostream.domain.TableSnapshot;
import com.satyendra.restostream.repository.TableSnapshotRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Closes the last correctness gap in the timer subsystem.</p>
 *
 * <p>The orchestration layer deliberately persists a transition before arming its deadline, because
 * the compare-and-set is what adjudicates concurrent writers. That ordering leaves one narrow
 * window: an instance can be killed, or Redis can reject the arm, after the new state is durable
 * but before its timer exists. The snapshot would then claim an armed SLA that nothing is counting
 * down — a table quietly excluded from alerting.</p>
 *
 * <p>This reconciler makes that window self-healing. On a fixed cadence it walks the floor plan and
 * repairs drift in both directions:</p>
 * <ul>
 *   <li>A snapshot with an armed, un-breached SLA but no entry in the deadline index is
 *       <strong>re-armed</strong>. If its deadline has already passed it is armed with a minimal
 *       lifetime so that it fires on the very next detection pass rather than waiting out a fresh
 *       window.</li>
 *   <li>A snapshot with no armed SLA that nonetheless has an entry in the deadline index has that
 *       entry <strong>cancelled</strong>, so an operator is never paged about an SLA the table is
 *       no longer subject to.</li>
 * </ul>
 *
 * <p>Like the sweeper, it is safe to run on every instance: every repair is an idempotent write of
 * a value derived from the authoritative snapshot, so concurrent reconcilers converge rather than
 * fight.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@Component
public class TimerDriftReconciler implements SmartLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(TimerDriftReconciler.class);
    private static final int LIFECYCLE_PHASE = Integer.MAX_VALUE - 2_000;

    /** Lifetime used when re-arming a deadline that is already overdue. */
    private static final Duration IMMEDIATE_TTL = Duration.ofMillis(1);

    private final TableSnapshotRepository repository;
    private final DistributedTimerService timerService;
    private final RestoStreamProperties properties;
    private final Clock clock;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile Disposable subscription;

    /**
     * Creates the reconciler.
     *
     * @param repository   the authoritative snapshot store
     * @param timerService the deadline index being reconciled against
     * @param properties   application configuration supplying the cadence
     * @param clock        the application clock
     */
    public TimerDriftReconciler(TableSnapshotRepository repository,
                                DistributedTimerService timerService,
                                RestoStreamProperties properties,
                                Clock clock) {
        this.repository = Objects.requireNonNull(repository, "repository must not be null");
        this.timerService = Objects.requireNonNull(timerService, "timerService must not be null");
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public void start() {
        RestoStreamProperties.Timers timers = properties.timers();
        if (!timers.driftReconcileEnabled()) {
            LOG.warn("timer drift reconciliation is disabled by configuration; a crash between "
                    + "persisting a transition and arming its deadline would go unrepaired");
            return;
        }
        if (!running.compareAndSet(false, true)) {
            return;
        }
        Duration interval = timers.driftReconcileInterval();
        LOG.info("starting timer drift reconciliation every {}", interval);
        subscription = Flux.interval(interval, interval, Schedulers.parallel())
                .onBackpressureDrop()
                .concatMap(tick -> reconcileOnce()
                        .onErrorResume(error -> {
                            LOG.error("timer drift reconciliation failed; will retry next pass", error);
                            return Mono.empty();
                        }))
                .subscribe();
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        Disposable current = subscription;
        if (current != null) {
            current.dispose();
            subscription = null;
        }
        LOG.info("timer drift reconciliation stopped");
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return LIFECYCLE_PHASE;
    }

    /**
     * Performs exactly one reconciliation pass. Exposed so that integration tests can assert the
     * repair behaviour deterministically instead of waiting on wall-clock ticks.
     *
     * @return a report of what was inspected and repaired
     */
    public Mono<ReconciliationReport> reconcileOnce() {
        return Mono.defer(() -> {
            Instant now = clock.instant();
            AtomicLong inspected = new AtomicLong();
            AtomicLong rearmed = new AtomicLong();
            AtomicLong cancelled = new AtomicLong();
            return repository.findAll()
                    .doOnNext(snapshot -> inspected.incrementAndGet())
                    .concatMap(snapshot -> reconcile(snapshot, now, rearmed, cancelled))
                    .then(Mono.fromSupplier(() -> new ReconciliationReport(
                            inspected.get(), rearmed.get(), cancelled.get())))
                    .doOnNext(report -> {
                        if (report.repaired() > 0L) {
                            LOG.warn("timer drift reconciliation repaired {} deadline(s): "
                                            + "{} re-armed, {} cancelled (of {} table(s) inspected)",
                                    report.repaired(), report.rearmed(),
                                    report.cancelled(), report.inspected());
                        } else {
                            LOG.debug("timer drift reconciliation found no drift across {} table(s)",
                                    report.inspected());
                        }
                    });
        });
    }

    private Mono<Void> reconcile(TableSnapshot snapshot,
                                 Instant now,
                                 AtomicLong rearmed,
                                 AtomicLong cancelled) {
        boolean shouldBeArmed = snapshot.slaArmed() && !snapshot.slaBreached();
        return timerService.deadlineOf(snapshot.tableId())
                .map(Optional::of)
                .defaultIfEmpty(Optional.empty())
                .flatMap(indexed -> {
                    if (shouldBeArmed && indexed.isEmpty()) {
                        Duration ttl = remainingTtl(snapshot.slaDeadline(), now);
                        rearmed.incrementAndGet();
                        LOG.warn("table={} claims an armed {} SLA due {} but no deadline was indexed; "
                                        + "re-arming with ttl={}",
                                snapshot.tableId(), snapshot.slaKind(), snapshot.slaDeadline(), ttl);
                        return timerService.arm(snapshot.tableId(), snapshot.slaKind(),
                                ttl, snapshot.slaDeadline());
                    }
                    if (!shouldBeArmed && indexed.isPresent()) {
                        cancelled.incrementAndGet();
                        LOG.warn("table={} is {} with no active SLA but a deadline was still indexed "
                                        + "at {}; cancelling",
                                snapshot.tableId(), snapshot.state(), indexed.get());
                        return timerService.cancel(snapshot.tableId()).then();
                    }
                    return Mono.empty();
                });
    }

    private static Duration remainingTtl(Instant deadline, Instant now) {
        Duration remaining = Duration.between(now, deadline);
        return remaining.isZero() || remaining.isNegative() ? IMMEDIATE_TTL : remaining;
    }

    /**
     * Outcome of one reconciliation pass.
     *
     * @param inspected number of tables walked
     * @param rearmed   number of missing deadlines re-armed
     * @param cancelled number of orphaned deadlines cancelled
     */
    public record ReconciliationReport(long inspected, long rearmed, long cancelled) {

        /**
         * @return the total number of repairs performed
         */
        public long repaired() {
            return rearmed + cancelled;
        }
    }

}
