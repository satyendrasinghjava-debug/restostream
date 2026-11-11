/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.ingestion;

import com.satyendra.restostream.domain.OccupancySignal;
import com.satyendra.restostream.engine.TransitionDecision;
import com.satyendra.restostream.service.SignalApplicationResult;
import com.satyendra.restostream.service.TableManagementService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Owns the inbound half of the pipeline: discovers every {@link OccupancySignalSource} bean in
 * the context, subscribes to each one, and drives the resulting signals through
 * {@link TableManagementService}.</p>
 *
 * <h2>Why each source is subscribed independently</h2>
 * <p>A naive implementation merges all sources into one {@link Flux} and subscribes once. That
 * couples their fates: one misbehaving camera adapter that terminates with an error takes the
 * manual-tap transport down with it, and the restaurant loses the ability to seat guests because a
 * camera broke. Here every source gets its own subscription, its own retry policy and its own
 * metrics — a failing transport degrades only itself.</p>
 *
 * <h2>Failure containment within a source</h2>
 * <p>Inside a subscription, a signal that the engine rejects, or whose persistence fails, must not
 * terminate the stream either. Each signal is therefore processed in its own inner {@link Mono}
 * whose errors are logged and absorbed, which keeps the outer subscription alive for the life of
 * the application. Only an error raised by the <em>source itself</em> reaches the retry policy,
 * where the transport is resubscribed with exponential backoff.</p>
 *
 * <p>Declared as a {@link SmartLifecycle} bean in
 * {@link com.satyendra.restostream.config.IngestionConfig} rather than component-scanned, because its
 * tuning parameters are primitives bound from configuration. It starts only once the Redis
 * connection factory, the state engine and the broadcaster are all fully initialised, and stops
 * cleanly before they are torn down.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public class SignalIngestionGateway implements SmartLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(SignalIngestionGateway.class);

    /** Started late and stopped early relative to infrastructure beans. */
    private static final int LIFECYCLE_PHASE = Integer.MAX_VALUE - 1_000;

    private final List<OccupancySignalSource> sources;
    private final TableManagementService tableManagementService;
    private final MeterRegistry meterRegistry;
    private final Clock clock;
    private final int concurrencyPerSource;
    private final Duration housekeepingInterval;

    private final List<Disposable> subscriptions = new CopyOnWriteArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * Creates the gateway.
     *
     * @param sources                every signal source bean in the context; may be empty
     * @param tableManagementService the orchestration service signals are driven through
     * @param meterRegistry          registry for ingestion counters
     * @param clock                  clock used for housekeeping
     * @param concurrencyPerSource   how many signals from one source may be in flight at once
     * @param housekeepingInterval   cadence of {@link OccupancySignalSource#housekeep}
     */
    public SignalIngestionGateway(List<OccupancySignalSource> sources,
                                  TableManagementService tableManagementService,
                                  MeterRegistry meterRegistry,
                                  Clock clock,
                                  int concurrencyPerSource,
                                  Duration housekeepingInterval) {
        this.sources = List.copyOf(Objects.requireNonNull(sources, "sources must not be null"));
        this.tableManagementService =
                Objects.requireNonNull(tableManagementService, "tableManagementService must not be null");
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        if (concurrencyPerSource <= 0) {
            throw new IllegalArgumentException(
                    "concurrencyPerSource must be positive but was " + concurrencyPerSource);
        }
        this.concurrencyPerSource = concurrencyPerSource;
        this.housekeepingInterval =
                Objects.requireNonNull(housekeepingInterval, "housekeepingInterval must not be null");
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        List<OccupancySignalSource> active = sources.stream()
                .filter(OccupancySignalSource::enabled)
                .toList();

        LOG.info("RestoStream ingestion starting with {} of {} source(s): {}",
                active.size(), sources.size(), active.stream().map(OccupancySignalSource::sourceId).toList());

        active.forEach(source -> subscriptions.add(subscribe(source)));

        if (!active.isEmpty() && !housekeepingInterval.isZero() && !housekeepingInterval.isNegative()) {
            subscriptions.add(startHousekeeping(active));
        }
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        LOG.info("RestoStream ingestion stopping; disposing {} subscription(s)", subscriptions.size());
        subscriptions.forEach(Disposable::dispose);
        subscriptions.clear();
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
     * @return the identifiers of every source this gateway is aware of, enabled or not
     */
    public List<String> registeredSourceIds() {
        return sources.stream().map(OccupancySignalSource::sourceId).toList();
    }

    private Disposable subscribe(OccupancySignalSource source) {
        return source.signals()
                .flatMap(signal -> process(source, signal), concurrencyPerSource)
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(250))
                        .maxBackoff(Duration.ofSeconds(30))
                        .doBeforeRetry(retry -> LOG.error(
                                "ingestion source '{}' failed and will be resubscribed (attempt {})",
                                source.sourceId(), retry.totalRetries() + 1, retry.failure())))
                .doOnComplete(() -> LOG.warn(
                        "ingestion source '{}' completed; it will produce no further signals",
                        source.sourceId()))
                .subscribe(
                        result -> { },
                        error -> LOG.error("ingestion source '{}' terminated unrecoverably",
                                source.sourceId(), error));
    }

    private Mono<SignalApplicationResult> process(OccupancySignalSource source,
                                                  OccupancySignal signal) {
        return tableManagementService.applySignal(signal)
                .doOnNext(result -> record(source, result))
                .onErrorResume(error -> {
                    count(source, "error");
                    LOG.error("source={} table={} signal={} could not be applied",
                            source.sourceId(), signal.tableId(), signal.signalId(), error);
                    return Mono.empty();
                });
    }

    private void record(OccupancySignalSource source, SignalApplicationResult result) {
        TransitionDecision decision = result.decision();
        switch (decision) {
            case TransitionDecision.Applied applied -> {
                count(source, "applied");
                LOG.info("source={} applied: {}", source.sourceId(), applied.narrative());
            }
            case TransitionDecision.Ignored duplicate -> {
                count(source, "ignored");
                LOG.debug("source={} ignored: {}", source.sourceId(), duplicate.narrative());
            }
            case TransitionDecision.Rejected rejected -> {
                count(source, "rejected");
                LOG.info("source={} rejected: {}", source.sourceId(), rejected.narrative());
            }
        }
    }

    private void count(OccupancySignalSource source, String outcome) {
        Counter.builder("restostream.ingestion.signals")
                .description("Occupancy signals processed by the ingestion gateway")
                .tag("source", source.sourceId())
                .tag("kind", source.kind().name())
                .tag("outcome", outcome)
                .register(meterRegistry)
                .increment();
    }

    private Disposable startHousekeeping(List<OccupancySignalSource> active) {
        return Flux.interval(housekeepingInterval, housekeepingInterval, Schedulers.parallel())
                .onBackpressureDrop()
                .doOnNext(tick -> active.forEach(source -> {
                    try {
                        int reclaimed = source.housekeep(clock.instant());
                        if (reclaimed > 0) {
                            LOG.debug("source={} reclaimed {} idle tracking entr{}",
                                    source.sourceId(), reclaimed, reclaimed == 1 ? "y" : "ies");
                        }
                    } catch (RuntimeException ex) {
                        LOG.warn("housekeeping failed for source '{}'", source.sourceId(), ex);
                    }
                }))
                .subscribe();
    }
}
