/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.timer;

import com.satyendra.restostream.config.RestoStreamProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>The low-latency breach detection path. Subscribes to Redis key-expiry notifications and, for
 * each expired SLA timer key, attempts to claim the deadline. A successful claim is published to the
 * {@link SlaTimerFiringBus} as a {@link FiringSource#KEYSPACE_EXPIRY} firing.</p>
 *
 * <h2>Operational prerequisite</h2>
 * <p>Redis publishes these events only when {@code notify-keyspace-events} includes the {@code Ex}
 * flags. The shipped {@code docker-compose.yml} and {@code redis.conf} set it. If an operator
 * removes it, RestoStream keeps working correctly — the reconciliation sweeper still detects every
 * breach — but detection latency degrades from milliseconds to the sweep interval. That graceful
 * degradation is deliberate: a misconfigured cache must never be able to silence service alerts.</p>
 *
 * <h2>Expired keys carry no value</h2>
 * <p>The notification payload is the key <em>name</em> only; the value is already gone by the time
 * the event is published. This listener therefore extracts the table identifier from the key and
 * relies on {@link DistributedTimerService#claim} to recover the SLA class from the sibling
 * metadata hash.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@Component
public class SlaExpiryListener implements SmartLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(SlaExpiryListener.class);
    private static final int LIFECYCLE_PHASE = Integer.MAX_VALUE - 2_000;

    private final ReactiveStringRedisTemplate redis;
    private final DistributedTimerService timerService;
    private final SlaTimerFiringBus firingBus;
    private final RestoStreamProperties properties;
    private final Clock clock;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile Disposable subscription;

    /**
     * Creates the listener.
     *
     * @param redis        reactive string template used for the pub/sub subscription
     * @param timerService the timer service used to claim elapsed deadlines
     * @param firingBus    the bus claimed firings are published to
     * @param properties   application configuration
     * @param clock        clock supplying the claim reference instant
     */
    public SlaExpiryListener(ReactiveStringRedisTemplate redis,
                             DistributedTimerService timerService,
                             SlaTimerFiringBus firingBus,
                             RestoStreamProperties properties,
                             Clock clock) {
        this.redis = Objects.requireNonNull(redis, "redis must not be null");
        this.timerService = Objects.requireNonNull(timerService, "timerService must not be null");
        this.firingBus = Objects.requireNonNull(firingBus, "firingBus must not be null");
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public void start() {
        if (!properties.timers().keyspaceNotificationsEnabled()) {
            LOG.warn("Redis keyspace expiry notifications are disabled by configuration; "
                    + "SLA breach detection will rely solely on the reconciliation sweep");
            return;
        }
        if (!running.compareAndSet(false, true)) {
            return;
        }
        String pattern = properties.timers().keyspacePattern();
        LOG.info("subscribing to Redis keyspace expiry notifications on pattern '{}'", pattern);
        subscription = expiredKeys(pattern)
                .concatMap(this::onExpiredKey)
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(500))
                        .maxBackoff(Duration.ofSeconds(30))
                        .doBeforeRetry(retry -> LOG.error(
                                "keyspace expiry subscription failed; resubscribing (attempt {})",
                                retry.totalRetries() + 1, retry.failure())))
                .subscribe(
                        claimed -> { },
                        error -> LOG.error("keyspace expiry subscription terminated unrecoverably", error));
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
        LOG.info("keyspace expiry subscription disposed");
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return LIFECYCLE_PHASE;
    }

    private Flux<String> expiredKeys(String pattern) {
        return redis.listenToPatternLater(pattern)
                .flatMapMany(messages -> Flux.from(messages)
                        .map(ReactiveSubscription.Message::getMessage));
    }

    private Mono<SlaTimerFiring> onExpiredKey(String expiredKey) {
        Optional<String> tableId = properties.redis().tableIdFromSlaTimerKey(expiredKey);
        if (tableId.isEmpty()) {
            return Mono.empty();
        }
        String table = tableId.get();
        return timerService.claim(table, clock.instant())
                .map(slaKind -> new SlaTimerFiring(
                        table, slaKind, clock.instant(), FiringSource.KEYSPACE_EXPIRY))
                .doOnNext(firingBus::publish)
                .doOnNext(firing -> LOG.debug("claimed {} breach for table={} via keyspace expiry",
                        firing.slaKind(), firing.tableId()))
                .onErrorResume(error -> {
                    LOG.error("failed to claim expired SLA timer for table={}", table, error);
                    return Mono.empty();
                });
    }
}
