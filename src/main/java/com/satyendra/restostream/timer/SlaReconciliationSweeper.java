/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.timer;

import com.satyendra.restostream.config.RestoStreamProperties;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>The lossless breach detection path, and the reason RestoStream can make a delivery promise
 * about SLA alerts at all.</p>
 *
 * <p>On a fixed cadence it range-queries the deadline sorted set for entries scored at or before
 * now, and attempts to claim each one. Because the sorted set is <em>state</em> rather than a
 * message, no amount of pub/sub loss, subscriber disconnection, failover or misconfiguration can
 * make an overdue deadline invisible to this sweep. Its worst-case detection latency is one sweep
 * interval; its failure mode is lateness, never silence.</p>
 *
 * <p>It is safe to run on every instance simultaneously: all three instances may observe the same
 * overdue entry, but {@link DistributedTimerService#claim} is atomic, so exactly one of them is
 * granted the alert. No leader election, no distributed lock, no coordination service.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@Component
public class SlaReconciliationSweeper implements SmartLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(SlaReconciliationSweeper.class);
    private static final int LIFECYCLE_PHASE = Integer.MAX_VALUE - 2_000;

    private final DistributedTimerService timerService;
    private final SlaTimerFiringBus firingBus;
    private final RestoStreamProperties properties;
    private final Clock clock;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final LongAdder sweeps = new LongAdder();
    private final LongAdder claimed = new LongAdder();

    private volatile Disposable subscription;

    /**
     * Creates the sweeper.
     *
     * @param timerService the timer service queried and claimed against
     * @param firingBus    the bus claimed firings are published to
     * @param properties   application configuration supplying cadence and batch size
     * @param clock        clock supplying the sweep reference instant
     */
    public SlaReconciliationSweeper(DistributedTimerService timerService,
                                    SlaTimerFiringBus firingBus,
                                    RestoStreamProperties properties,
                                    Clock clock) {
        this.timerService = Objects.requireNonNull(timerService, "timerService must not be null");
        this.firingBus = Objects.requireNonNull(firingBus, "firingBus must not be null");
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public void start() {
        RestoStreamProperties.Timers timers = properties.timers();
        if (!timers.sweepEnabled()) {
            LOG.warn("SLA reconciliation sweep is disabled by configuration; breach detection now "
                    + "depends entirely on Redis keyspace notifications being delivered");
            return;
        }
        if (!running.compareAndSet(false, true)) {
            return;
        }
        Duration interval = timers.sweepInterval();
        LOG.info("starting SLA reconciliation sweep every {} with a batch size of {}",
                interval, timers.sweepBatchSize());

        subscription = Flux.interval(interval, interval, Schedulers.parallel())
                .onBackpressureDrop(dropped -> LOG.warn(
                        "skipped SLA sweep tick {} because the previous sweep was still running",
                        dropped))
                .concatMap(tick -> sweepOnce()
                        .onErrorResume(error -> {
                            LOG.error("SLA reconciliation sweep failed; will retry next tick", error);
                            return Mono.just(0L);
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
        LOG.info("SLA reconciliation sweep stopped after {} sweep(s) and {} claim(s)",
                sweeps.sum(), claimed.sum());
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
     * Performs exactly one sweep. Exposed so that integration tests can drive reconciliation
     * deterministically instead of waiting on wall-clock ticks.
     *
     * @return the number of deadlines this instance successfully claimed
     */
    public Mono<Long> sweepOnce() {
        return Mono.defer(() -> {
            sweeps.increment();
            Instant now = clock.instant();
            return timerService.findOverdue(now, properties.timers().sweepBatchSize())
                    .concatMap(tableId -> claimOverdue(tableId, now))
                    .count()
                    .doOnNext(count -> {
                        if (count > 0L) {
                            claimed.add(count);
                            LOG.info("SLA reconciliation sweep claimed {} overdue deadline(s)", count);
                        }
                    });
        });
    }

    /**
     * @return the total number of sweeps performed since startup
     */
    public long sweepCount() {
        return sweeps.sum();
    }

    /**
     * @return the total number of deadlines claimed by this instance's sweeps since startup
     */
    public long claimCount() {
        return claimed.sum();
    }

    private Mono<SlaTimerFiring> claimOverdue(String tableId, Instant now) {
        return timerService.claim(tableId, now)
                .map(slaKind -> new SlaTimerFiring(
                        tableId, slaKind, now, FiringSource.RECONCILIATION_SWEEP))
                .doOnNext(firingBus::publish)
                .onErrorResume(error -> {
                    LOG.error("failed to claim overdue SLA deadline for table={}", tableId, error);
                    return Mono.empty();
                });
    }
}
