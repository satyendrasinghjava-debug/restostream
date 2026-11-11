/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.broadcast;

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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Broadcasts the live countdown for every table carrying an armed SLA, satisfying the "broadcast
 * state <strong>and timer changes</strong>" half of the live broadcaster requirement.</p>
 *
 * <p>The server is the authority on remaining time, not the browser. A dashboard that counted down
 * locally from the deadline in the last state message would drift with the operator's machine clock,
 * and — more importantly — a dashboard opened <em>after</em> a table was seated would have no
 * deadline to count down from at all. Publishing the countdown from the service means every screen
 * in the restaurant shows the same number at the same moment, however long it has been connected.</p>
 *
 * <p>Only armed, tracked tables are published, so an empty restaurant produces no traffic at all.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@Component
public class SlaCountdownPublisher implements SmartLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(SlaCountdownPublisher.class);
    private static final int LIFECYCLE_PHASE = Integer.MAX_VALUE - 1_500;

    private final TableSnapshotRepository repository;
    private final DashboardBroadcaster broadcaster;
    private final RestoStreamProperties properties;
    private final Clock clock;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile Disposable subscription;

    /**
     * Creates the publisher.
     *
     * @param repository  the authoritative snapshot store
     * @param broadcaster the dashboard fan-out
     * @param properties  application configuration supplying the cadence
     * @param clock       the application clock
     */
    public SlaCountdownPublisher(TableSnapshotRepository repository,
                                 DashboardBroadcaster broadcaster,
                                 RestoStreamProperties properties,
                                 Clock clock) {
        this.repository = Objects.requireNonNull(repository, "repository must not be null");
        this.broadcaster = Objects.requireNonNull(broadcaster, "broadcaster must not be null");
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public void start() {
        RestoStreamProperties.Timers timers = properties.timers();
        if (!timers.countdownEnabled()) {
            LOG.info("live SLA countdown broadcasting is disabled by configuration");
            return;
        }
        if (!running.compareAndSet(false, true)) {
            return;
        }
        Duration interval = timers.countdownInterval();
        LOG.info("broadcasting live SLA countdowns every {} on {}",
                interval, properties.stomp().timersTopic());
        subscription = Flux.interval(interval, interval, Schedulers.parallel())
                .onBackpressureDrop()
                .concatMap(tick -> publishOnce()
                        .onErrorResume(error -> {
                            LOG.warn("could not publish the SLA countdown tick", error);
                            return Mono.just(0);
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
        LOG.info("live SLA countdown broadcasting stopped");
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
     * Publishes exactly one countdown tick. Exposed so that integration tests can trigger it
     * deterministically.
     *
     * @return the number of tables included in the tick
     */
    public Mono<Integer> publishOnce() {
        return Mono.defer(() -> {
            Instant now = clock.instant();
            return repository.findAll()
                    .filter(TableSnapshot::slaArmed)
                    .map(snapshot -> TimerTick.from(snapshot, now))
                    .sort(java.util.Comparator.comparing(TimerTick::remainingMillis))
                    .collectList()
                    .doOnNext(broadcaster::broadcastTimerTicks)
                    .map(List::size);
        });
    }
}
