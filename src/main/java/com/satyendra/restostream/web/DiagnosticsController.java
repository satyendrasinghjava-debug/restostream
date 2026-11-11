/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.web;

import com.satyendra.restostream.ingestion.SignalIngestionGateway;
import com.satyendra.restostream.repository.TableSnapshotRepository;
import com.satyendra.restostream.stomp.StompSessionRegistry;
import com.satyendra.restostream.timer.DistributedTimerService;
import com.satyendra.restostream.timer.SlaReconciliationSweeper;
import com.satyendra.restostream.timer.TimerDriftReconciler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Operational visibility into the parts of RestoStream that are otherwise invisible: how many
 * dashboards are attached, how many SLA deadlines are armed cluster-wide, and whether the two
 * independent breach-detection paths are both doing their job.</p>
 *
 * <p>The two {@code POST} endpoints force a single sweep or reconciliation pass on demand. They
 * exist for two concrete reasons: an operator investigating a suspected missed alert can prove in
 * one call whether the deadline index agrees with the snapshot store, and the integration suite can
 * drive reconciliation deterministically instead of sleeping through a wall-clock interval. Both are
 * idempotent and safe to call in production.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@RestController
@RequestMapping("/api/v1/diagnostics")
public class DiagnosticsController {

    private final StompSessionRegistry sessionRegistry;
    private final DistributedTimerService timerService;
    private final TableSnapshotRepository repository;
    private final SignalIngestionGateway ingestionGateway;
    private final SlaReconciliationSweeper sweeper;
    private final TimerDriftReconciler driftReconciler;
    private final Clock clock;

    /**
     * Creates the controller.
     *
     * @param sessionRegistry  the STOMP routing table
     * @param timerService     the distributed timer subsystem
     * @param repository       the snapshot store
     * @param ingestionGateway the ingestion gateway
     * @param sweeper          the reconciliation sweeper
     * @param driftReconciler  the timer drift reconciler
     * @param clock            the application clock
     */
    public DiagnosticsController(StompSessionRegistry sessionRegistry,
                                 DistributedTimerService timerService,
                                 TableSnapshotRepository repository,
                                 SignalIngestionGateway ingestionGateway,
                                 SlaReconciliationSweeper sweeper,
                                 TimerDriftReconciler driftReconciler,
                                 Clock clock) {
        this.sessionRegistry = Objects.requireNonNull(sessionRegistry, "sessionRegistry must not be null");
        this.timerService = Objects.requireNonNull(timerService, "timerService must not be null");
        this.repository = Objects.requireNonNull(repository, "repository must not be null");
        this.ingestionGateway = Objects.requireNonNull(ingestionGateway, "ingestionGateway must not be null");
        this.sweeper = Objects.requireNonNull(sweeper, "sweeper must not be null");
        this.driftReconciler = Objects.requireNonNull(driftReconciler, "driftReconciler must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    /**
     * Returns a point-in-time operational snapshot of the service.
     *
     * @return the diagnostics payload
     */
    @GetMapping
    public Mono<Diagnostics> diagnostics() {
        return Mono.zip(repository.count(), timerService.armedCount())
                .map(counts -> new Diagnostics(
                        clock.instant(),
                        counts.getT1(),
                        counts.getT2(),
                        sessionRegistry.sessionCount(),
                        ingestionGateway.registeredSourceIds(),
                        ingestionGateway.isRunning(),
                        sweeper.sweepCount(),
                        sweeper.claimCount()));
    }

    /**
     * Forces one reconciliation sweep over the deadline index.
     *
     * @return the number of overdue deadlines this instance claimed
     */
    @PostMapping("/sweep")
    public Mono<SweepResult> sweepNow() {
        return sweeper.sweepOnce().map(claimed -> new SweepResult(claimed, clock.instant()));
    }

    /**
     * Forces one timer drift reconciliation pass.
     *
     * @return the repair report
     */
    @PostMapping("/reconcile-timers")
    public Mono<TimerDriftReconciler.ReconciliationReport> reconcileTimersNow() {
        return driftReconciler.reconcileOnce();
    }

    /**
     * Point-in-time operational snapshot.
     *
     * @param at                 the instant the snapshot was taken
     * @param provisionedTables  tables present in the snapshot store
     * @param armedSlaTimers     SLA deadlines armed cluster-wide
     * @param attachedDashboards dashboards currently connected over STOMP
     * @param ingestionSources   identifiers of every registered transport
     * @param ingestionRunning   whether the ingestion gateway is consuming
     * @param sweepsPerformed    reconciliation sweeps performed by this instance
     * @param deadlinesClaimed   deadlines claimed by this instance's sweeps
     */
    public record Diagnostics(Instant at,
                              long provisionedTables,
                              long armedSlaTimers,
                              int attachedDashboards,
                              List<String> ingestionSources,
                              boolean ingestionRunning,
                              long sweepsPerformed,
                              long deadlinesClaimed) {
    }

    /**
     * Result of a forced sweep.
     *
     * @param claimed the number of overdue deadlines claimed
     * @param at      the instant the sweep ran
     */
    public record SweepResult(long claimed, Instant at) {
    }
}
