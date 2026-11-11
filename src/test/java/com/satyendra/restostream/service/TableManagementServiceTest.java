/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.service;

import com.satyendra.restostream.alert.SlaAlert;
import com.satyendra.restostream.broadcast.DashboardBroadcaster;
import com.satyendra.restostream.config.RestoStreamProperties;
import com.satyendra.restostream.domain.OccupancySignal;
import com.satyendra.restostream.domain.SignalType;
import com.satyendra.restostream.domain.SlaKind;
import com.satyendra.restostream.domain.TableSnapshot;
import com.satyendra.restostream.domain.TableState;
import com.satyendra.restostream.engine.InvalidTransitionException;
import com.satyendra.restostream.engine.TableStateEngine;
import com.satyendra.restostream.engine.TransitionDecision;
import com.satyendra.restostream.repository.TableSnapshotRepository;
import com.satyendra.restostream.support.Fixtures;
import com.satyendra.restostream.timer.DistributedTimerService;
import com.satyendra.restostream.timer.FiringSource;
import com.satyendra.restostream.timer.SlaTimerFiring;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Unit suite for the orchestration layer, with Mockito standing in for Redis, the timer
 * subsystem and the broadcaster.</p>
 *
 * <p>The assertions here are mostly about <strong>ordering and contention</strong>, because that is
 * where an orchestrator earns its keep. Persisting before arming, re-deciding rather than replaying
 * on a lost compare-and-set, broadcasting only after a durable write, and surviving a timer
 * subsystem failure without rolling back a committed transition are each pinned by a test.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("TableManagementService")
class TableManagementServiceTest {

    @Mock
    private TableSnapshotRepository repository;

    @Mock
    private DistributedTimerService timerService;

    @Mock
    private DashboardBroadcaster broadcaster;

    private Clock clock;
    private TableStateEngine engine;
    private TableManagementService service;

    @BeforeEach
    void setUp() {
        clock = Fixtures.clockAt(Fixtures.T0);
        engine = new TableStateEngine(Fixtures.defaultPolicy());
        service = new TableManagementService(repository, engine, timerService, broadcaster, clock);
    }

    // ------------------------------------------------------------------ //

    @Nested
    @DisplayName("applying a signal")
    class ApplyingASignal {

        @Test
        @DisplayName("persists with a compare-and-set, then arms the timer, then broadcasts")
        void ordersSideEffectsCorrectly() {
            TableSnapshot free = Fixtures.freeTable();
            Mockito.when(repository.findById(Fixtures.TABLE_ID)).thenReturn(Mono.just(free));
            Mockito.when(repository.compareAndSave(any(), eq(0L))).thenReturn(Mono.just(true));
            Mockito.when(timerService.arm(any(), any(), any(), any())).thenReturn(Mono.empty());

            OccupancySignal signal =
                    Fixtures.signal(Fixtures.TABLE_ID, SignalType.GUEST_SEATED, Fixtures.T0, 2);

            StepVerifier.create(service.applySignal(signal))
                    .assertNext(result -> {
                        assertThat(result.applied()).isTrue();
                        assertThat(result.attempts()).isEqualTo(1);
                        assertThat(result.snapshot().state()).isEqualTo(TableState.SEATED);
                        assertThat(result.decision()).isInstanceOf(TransitionDecision.Applied.class);
                    })
                    .verifyComplete();

            InOrder order = Mockito.inOrder(repository, timerService, broadcaster);
            order.verify(repository).findById(Fixtures.TABLE_ID);
            order.verify(repository).compareAndSave(any(), eq(0L));
            order.verify(timerService).arm(eq(Fixtures.TABLE_ID), eq(SlaKind.WAITER_RESPONSE),
                    eq(Fixtures.WAITER_SLA), eq(Fixtures.T0.plus(Fixtures.WAITER_SLA)));
            order.verify(broadcaster).broadcastTableState(any());
            order.verifyNoMoreInteractions();
        }

        @Test
        @DisplayName("cancels the timer when a transition leaves the table unguarded")
        void cancelsTimerOnUnguardedTransition() {
            TableSnapshot uncleaned = Fixtures.uncleanedTable(Fixtures.T0, Fixtures.TURNAROUND_SLA);
            Mockito.when(repository.findById(Fixtures.TABLE_ID)).thenReturn(Mono.just(uncleaned));
            Mockito.when(repository.compareAndSave(any(), eq(uncleaned.version())))
                    .thenReturn(Mono.just(true));
            Mockito.when(timerService.cancel(Fixtures.TABLE_ID)).thenReturn(Mono.just(true));

            StepVerifier.create(service.applySignal(
                            Fixtures.signal(SignalType.CLEANING_COMPLETED, Fixtures.T0)))
                    .assertNext(result -> assertThat(result.snapshot().state())
                            .isEqualTo(TableState.FREE))
                    .verifyComplete();

            Mockito.verify(timerService).cancel(Fixtures.TABLE_ID);
            Mockito.verify(timerService, Mockito.never()).arm(any(), any(), any(), any());
        }

        @Test
        @DisplayName("writes nothing and broadcasts nothing for a duplicate assertion")
        void duplicateAssertionIsInert() {
            TableSnapshot seated = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);
            Mockito.when(repository.findById(Fixtures.TABLE_ID)).thenReturn(Mono.just(seated));

            StepVerifier.create(service.applySignal(
                            Fixtures.signal(SignalType.GUEST_SEATED, Fixtures.T0)))
                    .assertNext(result -> {
                        assertThat(result.applied()).isFalse();
                        assertThat(result.attempts())
                                .as("no write was attempted at all")
                                .isZero();
                        assertThat(result.decision())
                                .isInstanceOf(TransitionDecision.Ignored.class);
                    })
                    .verifyComplete();

            Mockito.verify(repository, Mockito.never()).compareAndSave(any(), anyLong());
            Mockito.verifyNoInteractions(timerService, broadcaster);
        }

        @Test
        @DisplayName("reports a rejection as data rather than as an error")
        void rejectionIsReportedAsData() {
            TableSnapshot uncleaned = Fixtures.uncleanedTable(Fixtures.T0, Fixtures.TURNAROUND_SLA);
            Mockito.when(repository.findById(Fixtures.TABLE_ID)).thenReturn(Mono.just(uncleaned));

            StepVerifier.create(service.applySignal(
                            Fixtures.signal(SignalType.GUEST_SEATED, Fixtures.T0)))
                    .assertNext(result -> assertThat(result.decision())
                            .isInstanceOf(TransitionDecision.Rejected.class))
                    .verifyComplete();

            Mockito.verifyNoInteractions(timerService, broadcaster);
        }

        @Test
        @DisplayName("fails with 404 semantics for an unprovisioned table")
        void unknownTableFails() {
            Mockito.when(repository.findById("T-404")).thenReturn(Mono.empty());

            StepVerifier.create(service.applySignal(
                            Fixtures.signal("T-404", SignalType.GUEST_SEATED, Fixtures.T0, 0)))
                    .expectErrorSatisfies(error -> assertThat(error)
                            .isInstanceOf(TableNotFoundException.class)
                            .hasMessageContaining("T-404"))
                    .verify();
        }

        @Test
        @DisplayName("rejects a null signal")
        void rejectsNullSignal() {
            StepVerifier.create(Mono.fromRunnable(() -> service.applySignal(null)))
                    .verifyError(NullPointerException.class);
        }
    }

    @Nested
    @DisplayName("optimistic concurrency")
    class OptimisticConcurrency {

        @Test
        @DisplayName("re-reads and re-decides when it loses a compare-and-set race")
        void retriesAfterLosingTheRace() {
            TableSnapshot free = Fixtures.freeTable();
            Mockito.when(repository.findById(Fixtures.TABLE_ID)).thenReturn(Mono.just(free));
            Mockito.when(repository.compareAndSave(any(), eq(0L)))
                    .thenReturn(Mono.just(false), Mono.just(true));
            Mockito.when(timerService.arm(any(), any(), any(), any())).thenReturn(Mono.empty());

            StepVerifier.create(service.applySignal(
                            Fixtures.signal(SignalType.GUEST_SEATED, Fixtures.T0)))
                    .assertNext(result -> assertThat(result.attempts()).isEqualTo(2))
                    .verifyComplete();

            Mockito.verify(repository, Mockito.times(2)).findById(Fixtures.TABLE_ID);
            Mockito.verify(repository, Mockito.times(2)).compareAndSave(any(), eq(0L));
            Mockito.verify(broadcaster, Mockito.times(1)).broadcastTableState(any());
        }

        @Test
        @DisplayName("re-decides rather than replaying, so a winner's move can turn a retry into a rejection")
        void retryRespectsTheWinnersMove() {
            TableSnapshot free = Fixtures.freeTable();
            TableSnapshot nowUncleaned =
                    Fixtures.uncleanedTable(Fixtures.T0, Fixtures.TURNAROUND_SLA);
            Mockito.when(repository.findById(Fixtures.TABLE_ID))
                    .thenReturn(Mono.just(free), Mono.just(nowUncleaned));
            Mockito.when(repository.compareAndSave(any(), eq(0L))).thenReturn(Mono.just(false));

            StepVerifier.create(service.applySignal(
                            Fixtures.signal(SignalType.GUEST_SEATED, Fixtures.T0)))
                    .assertNext(result -> assertThat(result.decision())
                            .as("the winning writer soiled the table, so seating is no longer legal")
                            .isInstanceOf(TransitionDecision.Rejected.class))
                    .verifyComplete();

            Mockito.verifyNoInteractions(timerService, broadcaster);
        }

        @Test
        @DisplayName("gives up with a retryable error once the attempt budget is exhausted")
        void exhaustsTheAttemptBudget() {
            Mockito.when(repository.findById(Fixtures.TABLE_ID))
                    .thenReturn(Mono.just(Fixtures.freeTable()));
            Mockito.when(repository.compareAndSave(any(), eq(0L))).thenReturn(Mono.just(false));

            StepVerifier.create(service.applySignal(
                            Fixtures.signal(SignalType.GUEST_SEATED, Fixtures.T0)))
                    .expectErrorSatisfies(error -> {
                        assertThat(error).isInstanceOf(ConcurrencyExhaustedException.class);
                        ConcurrencyExhaustedException contended = (ConcurrencyExhaustedException) error;
                        assertThat(contended.tableId()).isEqualTo(Fixtures.TABLE_ID);
                        assertThat(contended.attempts())
                                .isEqualTo(TableManagementService.MAX_CAS_ATTEMPTS);
                        assertThat(contended).hasMessageContaining("sustained concurrent modification");
                    })
                    .verify();

            Mockito.verify(repository, Mockito.times(TableManagementService.MAX_CAS_ATTEMPTS))
                    .compareAndSave(any(), eq(0L));
            Mockito.verifyNoInteractions(broadcaster);
        }
    }

    @Nested
    @DisplayName("timer subsystem failure")
    class TimerFailure {

        @Test
        @DisplayName("keeps a committed transition and still broadcasts when arming fails")
        void armingFailureDoesNotRollBack() {
            Mockito.when(repository.findById(Fixtures.TABLE_ID))
                    .thenReturn(Mono.just(Fixtures.freeTable()));
            Mockito.when(repository.compareAndSave(any(), eq(0L))).thenReturn(Mono.just(true));
            Mockito.when(timerService.arm(any(), any(), any(), any()))
                    .thenReturn(Mono.error(new IllegalStateException("redis unreachable")));

            StepVerifier.create(service.applySignal(
                            Fixtures.signal(SignalType.GUEST_SEATED, Fixtures.T0)))
                    .assertNext(result -> {
                        assertThat(result.applied())
                                .as("the write is already durable; the transition stands")
                                .isTrue();
                        assertThat(result.snapshot().state()).isEqualTo(TableState.SEATED);
                    })
                    .verifyComplete();

            Mockito.verify(broadcaster)
                    .broadcastTableState(Mockito.argThat(s -> s.state() == TableState.SEATED));
        }
    }

    @Nested
    @DisplayName("command semantics")
    class CommandSemantics {

        @Test
        @DisplayName("returns the resulting snapshot for an applied command")
        void appliedCommandReturnsNextState() {
            Mockito.when(repository.findById(Fixtures.TABLE_ID))
                    .thenReturn(Mono.just(Fixtures.freeTable()));
            Mockito.when(repository.compareAndSave(any(), eq(0L))).thenReturn(Mono.just(true));
            Mockito.when(timerService.arm(any(), any(), any(), any())).thenReturn(Mono.empty());

            StepVerifier.create(service.applySignalOrFail(
                            Fixtures.signal(SignalType.GUEST_SEATED, Fixtures.T0)))
                    .assertNext(snapshot -> assertThat(snapshot.state()).isEqualTo(TableState.SEATED))
                    .verifyComplete();
        }

        @Test
        @DisplayName("returns the unchanged snapshot for an ignored command")
        void ignoredCommandReturnsCurrentState() {
            TableSnapshot seated = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);
            Mockito.when(repository.findById(Fixtures.TABLE_ID)).thenReturn(Mono.just(seated));

            StepVerifier.create(service.applySignalOrFail(
                            Fixtures.signal(SignalType.GUEST_SEATED, Fixtures.T0)))
                    .expectNext(seated)
                    .verifyComplete();
        }

        @Test
        @DisplayName("raises a conflict for a rejected command")
        void rejectedCommandRaisesConflict() {
            TableSnapshot uncleaned = Fixtures.uncleanedTable(Fixtures.T0, Fixtures.TURNAROUND_SLA);
            Mockito.when(repository.findById(Fixtures.TABLE_ID)).thenReturn(Mono.just(uncleaned));

            StepVerifier.create(service.applySignalOrFail(
                            Fixtures.signal(SignalType.GUEST_SEATED, Fixtures.T0)))
                    .expectErrorSatisfies(error -> {
                        assertThat(error).isInstanceOf(InvalidTransitionException.class);
                        InvalidTransitionException invalid = (InvalidTransitionException) error;
                        assertThat(invalid.currentState()).isEqualTo(TableState.UNCLEANED);
                        assertThat(invalid.signalType()).isEqualTo(SignalType.GUEST_SEATED);
                    })
                    .verify();
        }
    }

    @Nested
    @DisplayName("SLA firing")
    class SlaFiring {

        private SlaTimerFiring firing(Instant at, FiringSource source) {
            return new SlaTimerFiring(Fixtures.TABLE_ID, SlaKind.WAITER_RESPONSE, at, source);
        }

        @Test
        @DisplayName("confirms a breach, persists the flag and broadcasts both state and alert")
        void confirmsAndBroadcasts() {
            TableSnapshot seated = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);
            Instant firedAt = seated.slaDeadline().plus(Duration.ofMillis(250));
            Mockito.when(repository.findById(Fixtures.TABLE_ID)).thenReturn(Mono.just(seated));
            Mockito.when(repository.compareAndSave(any(), eq(seated.version())))
                    .thenReturn(Mono.just(true));

            StepVerifier.create(service.processSlaFiring(
                            firing(firedAt, FiringSource.KEYSPACE_EXPIRY)))
                    .assertNext(alert -> {
                        assertThat(alert.tableId()).isEqualTo(Fixtures.TABLE_ID);
                        assertThat(alert.slaKind()).isEqualTo(SlaKind.WAITER_RESPONSE);
                        assertThat(alert.severity().name()).isEqualTo("CRITICAL");
                        assertThat(alert.detectedBy()).isEqualTo(FiringSource.KEYSPACE_EXPIRY);
                        assertThat(alert.overdueMillis()).isEqualTo(250L);
                        assertThat(alert.dwellMillis())
                                .isEqualTo(Fixtures.WAITER_SLA.toMillis() + 250L);
                        assertThat(alert.message()).contains(Fixtures.TABLE_ID);
                    })
                    .verifyComplete();

            ArgumentCaptor<TableSnapshot> stored = ArgumentCaptor.forClass(TableSnapshot.class);
            Mockito.verify(repository).compareAndSave(stored.capture(), eq(seated.version()));
            assertThat(stored.getValue().slaBreached()).isTrue();

            Mockito.verify(broadcaster)
                    .broadcastTableState(Mockito.argThat(TableSnapshot::slaBreached));
            Mockito.verify(broadcaster).broadcastSlaAlert(any(SlaAlert.class));
        }

        @Test
        @DisplayName("discards a stale firing without writing or broadcasting")
        void discardsStaleFiring() {
            TableSnapshot acknowledged = Fixtures.acknowledgedTable(
                    Fixtures.T0, Fixtures.T0.plus(Duration.ofMinutes(2)));
            Mockito.when(repository.findById(Fixtures.TABLE_ID))
                    .thenReturn(Mono.just(acknowledged));

            StepVerifier.create(service.processSlaFiring(firing(
                            Fixtures.T0.plus(Fixtures.WAITER_SLA), FiringSource.RECONCILIATION_SWEEP)))
                    .verifyComplete();

            Mockito.verify(repository, Mockito.never()).compareAndSave(any(), anyLong());
            Mockito.verifyNoInteractions(broadcaster);
        }

        @Test
        @DisplayName("retries the breach flag on a lost compare-and-set")
        void retriesBreachFlagOnConflict() {
            TableSnapshot seated = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);
            Instant firedAt = seated.slaDeadline();
            Mockito.when(repository.findById(Fixtures.TABLE_ID)).thenReturn(Mono.just(seated));
            Mockito.when(repository.compareAndSave(any(), eq(seated.version())))
                    .thenReturn(Mono.just(false), Mono.just(true));

            StepVerifier.create(service.processSlaFiring(
                            firing(firedAt, FiringSource.KEYSPACE_EXPIRY)))
                    .expectNextCount(1)
                    .verifyComplete();

            Mockito.verify(repository, Mockito.times(2))
                    .compareAndSave(any(), eq(seated.version()));
        }
    }

    @Nested
    @DisplayName("provisioning and queries")
    class ProvisioningAndQueries {

        @Test
        @DisplayName("provisions a genesis snapshot idempotently")
        void provisionsIdempotently() {
            Mockito.when(repository.provisionIfAbsent(any())).thenReturn(Mono.just(true));

            StepVerifier.create(service.provision(
                            new RestoStreamProperties.TableDefinition("T-07", "PATIO", 6)))
                    .expectNext(true)
                    .verifyComplete();

            ArgumentCaptor<TableSnapshot> captured = ArgumentCaptor.forClass(TableSnapshot.class);
            Mockito.verify(repository).provisionIfAbsent(captured.capture());
            assertThat(captured.getValue().tableId()).isEqualTo("T-07");
            assertThat(captured.getValue().zone()).isEqualTo("PATIO");
            assertThat(captured.getValue().seats()).isEqualTo(6);
            assertThat(captured.getValue().version()).isZero();
            assertThat(captured.getValue().state()).isEqualTo(TableState.FREE);
            assertThat(captured.getValue().stateSince()).isEqualTo(Fixtures.T0);
        }

        @Test
        @DisplayName("reports an existing table as not newly created")
        void existingTableIsNotRecreated() {
            Mockito.when(repository.provisionIfAbsent(any())).thenReturn(Mono.just(false));

            StepVerifier.create(service.provision(
                            new RestoStreamProperties.TableDefinition("T-07", "PATIO", 6)))
                    .expectNext(false)
                    .verifyComplete();
        }

        @Test
        @DisplayName("returns the floor plan sorted by table identifier")
        void floorPlanIsSorted() {
            Mockito.when(repository.findAll()).thenReturn(Flux.just(
                    TableSnapshot.provision("T-03", "MAIN", 4, Fixtures.T0),
                    TableSnapshot.provision("T-01", "WINDOW", 2, Fixtures.T0),
                    TableSnapshot.provision("T-02", "WINDOW", 2, Fixtures.T0)));

            StepVerifier.create(service.findFloorPlan().map(TableSnapshot::tableId))
                    .expectNext("T-01", "T-02", "T-03")
                    .verifyComplete();
        }

        @Test
        @DisplayName("fails a lookup for an unprovisioned table")
        void lookupFailsForUnknownTable() {
            Mockito.when(repository.findById("T-404")).thenReturn(Mono.empty());

            StepVerifier.create(service.findTable("T-404"))
                    .verifyError(TableNotFoundException.class);
        }

        @Test
        @DisplayName("returns a provisioned table")
        void lookupReturnsTable() {
            TableSnapshot free = Fixtures.freeTable();
            Mockito.when(repository.findById(Fixtures.TABLE_ID)).thenReturn(Mono.just(free));

            StepVerifier.create(service.findTable(Fixtures.TABLE_ID))
                    .expectNext(free)
                    .verifyComplete();
        }
    }
}
