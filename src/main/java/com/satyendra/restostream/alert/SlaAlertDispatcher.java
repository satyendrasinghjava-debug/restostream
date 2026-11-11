/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.alert;

import com.satyendra.restostream.service.TableManagementService;
import com.satyendra.restostream.timer.SlaTimerFiring;
import com.satyendra.restostream.timer.SlaTimerFiringBus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Consumes claimed SLA firings from the {@link SlaTimerFiringBus} and drives each one through
 * confirmation, persistence and broadcast. This component is what makes the SLA alert
 * <em>asynchronous</em> as specified: nothing in the request path waits on it, and the waiter who
 * seated the table five minutes ago is long gone from the event loop by the time it runs.</p>
 *
 * <p>Firings are processed with {@code concatMap} rather than {@code flatMap} — deliberately
 * serialised. Alerting throughput is measured in events per minute, never per millisecond, so there
 * is nothing to gain from concurrency here, while serialisation guarantees that two firings for the
 * same table can never interleave their compare-and-set loops and burn the attempt budget against
 * each other.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@Component
public class SlaAlertDispatcher implements SmartLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(SlaAlertDispatcher.class);
    private static final int LIFECYCLE_PHASE = Integer.MAX_VALUE - 1_500;

    private final SlaTimerFiringBus firingBus;
    private final TableManagementService tableManagementService;
    private final MeterRegistry meterRegistry;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile Disposable subscription;

    /**
     * Creates the dispatcher.
     *
     * @param firingBus              the bus claimed firings arrive on
     * @param tableManagementService the orchestration service that confirms and broadcasts
     * @param meterRegistry          registry for alert counters
     */
    public SlaAlertDispatcher(SlaTimerFiringBus firingBus,
                              TableManagementService tableManagementService,
                              MeterRegistry meterRegistry) {
        this.firingBus = Objects.requireNonNull(firingBus, "firingBus must not be null");
        this.tableManagementService =
                Objects.requireNonNull(tableManagementService, "tableManagementService must not be null");
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        LOG.info("SLA alert dispatcher subscribing to the timer firing bus");
        subscription = firingBus.firings()
                .concatMap(this::dispatch)
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(250))
                        .maxBackoff(Duration.ofSeconds(10))
                        .doBeforeRetry(retry -> LOG.error(
                                "SLA alert dispatcher failed and will resubscribe (attempt {})",
                                retry.totalRetries() + 1, retry.failure())))
                .subscribe(
                        alert -> { },
                        error -> LOG.error("SLA alert dispatcher terminated unrecoverably", error));
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
        LOG.info("SLA alert dispatcher stopped");
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return LIFECYCLE_PHASE;
    }

    private Mono<SlaAlert> dispatch(SlaTimerFiring firing) {
        return tableManagementService.processSlaFiring(firing)
                .doOnSuccess(alert -> {
                    // A Mono completing empty signals a firing the engine judged stale; doOnSuccess
                    // receives null in that case, which is the one place a null is meaningful here.
                    if (alert == null) {
                        count(firing, "stale", "NONE");
                    } else {
                        count(firing, "alerted", alert.severity().name());
                    }
                })
                .onErrorResume(error -> {
                    count(firing, "error", "NONE");
                    LOG.error("could not dispatch the {} breach alert for table={}",
                            firing.slaKind(), firing.tableId(), error);
                    return Mono.empty();
                });
    }

    private void count(SlaTimerFiring firing, String outcome, String severity) {
        Counter.builder("restostream.sla.firings")
                .description("Claimed SLA timer firings processed by the alert dispatcher")
                .tag("sla", firing.slaKind().name())
                .tag("detectedBy", firing.source().name())
                .tag("outcome", outcome)
                .tag("severity", severity)
                .register(meterRegistry)
                .increment();
    }
}
