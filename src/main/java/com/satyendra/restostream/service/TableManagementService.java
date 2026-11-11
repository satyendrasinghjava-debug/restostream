/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.service;

import com.satyendra.restostream.alert.SlaAlert;
import com.satyendra.restostream.broadcast.DashboardBroadcaster;
import com.satyendra.restostream.config.RestoStreamProperties;
import com.satyendra.restostream.domain.OccupancySignal;
import com.satyendra.restostream.domain.TableSnapshot;
import com.satyendra.restostream.engine.BreachDecision;
import com.satyendra.restostream.engine.TableStateEngine;
import com.satyendra.restostream.engine.TimerDirective;
import com.satyendra.restostream.engine.TransitionDecision;
import com.satyendra.restostream.repository.TableSnapshotRepository;
import com.satyendra.restostream.timer.DistributedTimerService;
import com.satyendra.restostream.timer.SlaTimerFiring;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>The orchestration layer, and the only component that is permitted to mutate table state. It
 * composes four collaborators that each know nothing of the others: the pure
 * {@link TableStateEngine} that decides, the {@link TableSnapshotRepository} that persists with a
 * compare-and-set, the {@link DistributedTimerService} that arms deadlines, and the
 * {@link DashboardBroadcaster} that fans out.</p>
 *
 * <h2>The ordering decision: persist, then arm, then broadcast</h2>
 * <p>Writing to Redis first is what makes the pipeline correct under concurrency. If the timer were
 * armed before the compare-and-set, every instance that <em>lost</em> the race would still have
 * armed a deadline for a transition that never happened. By letting the CAS adjudicate first, only
 * the winner performs side effects — and a loser simply re-reads and asks the engine again, which
 * is cheap precisely because the engine is a pure function.</p>
 *
 * <p>Broadcasting last means a dashboard never learns about a state that is not yet durable. The
 * remaining window — a crash after the CAS but before the timer is armed — is closed by
 * {@link com.satyendra.restostream.timer.TimerDriftReconciler}, not ignored.</p>
 *
 * <h2>Retry strategy</h2>
 * <p>CAS conflicts are retried by re-reading and <em>re-deciding</em> from scratch, never by
 * replaying the previously computed snapshot. That distinction matters: if the winning writer moved
 * the table to a state where our signal is no longer legal, the retry must produce a rejection, not
 * force through a transition the floor plan has outgrown.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@Service
public class TableManagementService {

    private static final Logger LOG = LoggerFactory.getLogger(TableManagementService.class);

    /** Attempt budget for the optimistic compare-and-set loop. */
    static final int MAX_CAS_ATTEMPTS = 8;

    private final TableSnapshotRepository repository;
    private final TableStateEngine engine;
    private final DistributedTimerService timerService;
    private final DashboardBroadcaster broadcaster;
    private final Clock clock;

    /**
     * Creates the service.
     *
     * @param repository   table persistence
     * @param engine       the pure state engine
     * @param timerService the distributed timer subsystem
     * @param broadcaster  the dashboard fan-out
     * @param clock        the application clock
     */
    public TableManagementService(TableSnapshotRepository repository,
                                  TableStateEngine engine,
                                  DistributedTimerService timerService,
                                  DashboardBroadcaster broadcaster,
                                  Clock clock) {
        this.repository = Objects.requireNonNull(repository, "repository must not be null");
        this.engine = Objects.requireNonNull(engine, "engine must not be null");
        this.timerService = Objects.requireNonNull(timerService, "timerService must not be null");
        this.broadcaster = Objects.requireNonNull(broadcaster, "broadcaster must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    /**
     * Applies one occupancy signal, reporting the decision without raising an error when the signal
     * was ignored or rejected. This is the entry point used by the asynchronous ingestion gateway,
     * for which a rejection is routine information rather than a failure.
     *
     * @param signal the signal to apply; must not be {@code null}
     * @return the outcome; errors only on infrastructure failure or exhausted contention
     */
    public Mono<SignalApplicationResult> applySignal(OccupancySignal signal) {
        Objects.requireNonNull(signal, "signal must not be null");
        return attempt(signal, 1);
    }

    /**
     * Applies one occupancy signal as an imperative command, raising
     * {@link com.satyendra.restostream.engine.InvalidTransitionException} when the state machine
     * refuses it. This is the entry point used by the REST and STOMP command boundaries, which must
     * surface a conflict to the operator.
     *
     * @param signal the signal to apply; must not be {@code null}
     * @return the authoritative table state after the command
     */
    public Mono<TableSnapshot> applySignalOrFail(OccupancySignal signal) {
        return applySignal(signal).flatMap(result -> switch (result.decision()) {
            case TransitionDecision.Rejected rejected -> Mono.error(rejected.toException());
            case TransitionDecision.Applied applied -> Mono.just(applied.next());
            case TransitionDecision.Ignored duplicate -> Mono.just(duplicate.current());
        });
    }

    /**
     * Confirms or discards a claimed SLA firing and, when the breach is real, persists the breach
     * flag and broadcasts the alert.
     *
     * @param firing the claimed firing; must not be {@code null}
     * @return the broadcast alert, or empty when the firing turned out to be stale
     */
    public Mono<SlaAlert> processSlaFiring(SlaTimerFiring firing) {
        Objects.requireNonNull(firing, "firing must not be null");
        return attemptBreach(firing, 1);
    }

    /**
     * Provisions one table if it is not already present. Idempotent, so restarting the service never
     * resets a live dining room.
     *
     * @param definition the table definition; must not be {@code null}
     * @return {@code true} when this call created the table
     */
    public Mono<Boolean> provision(RestoStreamProperties.TableDefinition definition) {
        Objects.requireNonNull(definition, "definition must not be null");
        return Mono.fromCallable(() -> TableSnapshot.provision(
                        definition.id(), definition.zone(), definition.seats(), clock.instant()))
                .flatMap(repository::provisionIfAbsent)
                .doOnNext(created -> {
                    if (created) {
                        LOG.info("provisioned table {} in zone {} with {} seat(s)",
                                definition.id(), definition.zone(), definition.seats());
                    }
                });
    }

    /**
     * Loads one table.
     *
     * @param tableId the table identifier; must not be {@code null}
     * @return the snapshot, or an error of {@link TableNotFoundException} when unprovisioned
     */
    public Mono<TableSnapshot> findTable(String tableId) {
        Objects.requireNonNull(tableId, "tableId must not be null");
        return repository.findById(tableId)
                .switchIfEmpty(Mono.error(() -> new TableNotFoundException(tableId)));
    }

    /**
     * Loads the whole floor plan, ordered by table identifier so that dashboards render a stable
     * layout between refreshes.
     *
     * @return every provisioned table
     */
    public Flux<TableSnapshot> findFloorPlan() {
        return repository.findAll().sort(java.util.Comparator.comparing(TableSnapshot::tableId));
    }

    private Mono<SignalApplicationResult> attempt(OccupancySignal signal, int attemptNumber) {
        if (attemptNumber > MAX_CAS_ATTEMPTS) {
            return Mono.error(new ConcurrencyExhaustedException(signal.tableId(), MAX_CAS_ATTEMPTS));
        }
        return findTable(signal.tableId()).flatMap(current -> {
            Instant now = clock.instant();
            TransitionDecision decision = engine.decide(current, signal, now);
            if (!(decision instanceof TransitionDecision.Applied applied)) {
                return Mono.just(new SignalApplicationResult(decision, current, attemptNumber - 1));
            }
            return repository.compareAndSave(applied.next(), current.version())
                    .flatMap(stored -> {
                        if (!stored) {
                            LOG.debug("CAS conflict on table={} attempt={}; re-deciding",
                                    signal.tableId(), attemptNumber);
                            return attempt(signal, attemptNumber + 1);
                        }
                        return executeTimer(applied)
                                .thenReturn(new SignalApplicationResult(
                                        applied, applied.next(), attemptNumber))
                                .doOnNext(result -> broadcaster.broadcastTableState(result.snapshot()));
                    });
        });
    }

    private Mono<Void> executeTimer(TransitionDecision.Applied applied) {
        TimerDirective timer = applied.timer();
        String tableId = applied.next().tableId();
        Mono<Void> action = switch (timer.action()) {
            case ARM -> timerService.arm(tableId, timer.slaKind(), timer.ttl(), timer.deadline());
            case CANCEL -> timerService.cancel(tableId).then();
            case NONE -> Mono.empty();
        };
        return action.onErrorResume(error -> {
            // State is already durable, so the transition itself stands. The drift reconciler will
            // re-arm the missing deadline on its next pass; failing the whole command here would
            // leave the caller believing a committed transition had not happened.
            LOG.error("table={} transitioned but its {} timer directive failed; "
                            + "the drift reconciler will repair it",
                    tableId, timer.action(), error);
            return Mono.empty();
        });
    }

    private Mono<SlaAlert> attemptBreach(SlaTimerFiring firing, int attemptNumber) {
        if (attemptNumber > MAX_CAS_ATTEMPTS) {
            return Mono.error(new ConcurrencyExhaustedException(firing.tableId(), MAX_CAS_ATTEMPTS));
        }
        return findTable(firing.tableId()).flatMap(current -> {
            BreachDecision decision = engine.evaluateBreach(current, firing.slaKind(), firing.firedAt());
            if (!(decision instanceof BreachDecision.Confirmed confirmed)) {
                LOG.debug("discarding stale SLA firing: {}", decision.narrative());
                return Mono.empty();
            }
            return repository.compareAndSave(confirmed.next(), current.version())
                    .flatMap(stored -> {
                        if (!stored) {
                            LOG.debug("CAS conflict while flagging breach on table={} attempt={}",
                                    firing.tableId(), attemptNumber);
                            return attemptBreach(firing, attemptNumber + 1);
                        }
                        SlaAlert alert = SlaAlert.from(confirmed.next(), firing);
                        LOG.warn("SLA BREACH [{}] {}", alert.severity(), confirmed.narrative());
                        broadcaster.broadcastTableState(confirmed.next());
                        broadcaster.broadcastSlaAlert(alert);
                        return Mono.just(alert);
                    });
        });
    }
}
