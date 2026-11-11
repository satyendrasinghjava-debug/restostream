/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.timer;

import com.satyendra.restostream.config.RestoStreamProperties;
import com.satyendra.restostream.domain.SlaKind;
import com.satyendra.restostream.domain.TableSnapshot;
import com.satyendra.restostream.domain.TableState;
import com.satyendra.restostream.repository.TableSnapshotRepository;
import com.satyendra.restostream.support.Fixtures;
import com.satyendra.restostream.support.MutableClock;
import com.satyendra.restostream.support.TestProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Unit suite for the distributed timer subsystem's reconciliation machinery.</p>
 *
 * <p>The Redis-backed {@link RedisDistributedTimerService} itself is verified against a real server
 * in the Testcontainers suite, because the behaviour that matters there — Lua atomicity, sorted-set
 * range semantics, key expiry — is precisely what a mock cannot tell the truth about. What <em>is</em>
 * worth mocking is the policy layered on top: which deadlines get claimed, and how drift between the
 * snapshot store and the deadline index is repaired. Those are decisions, not Redis behaviour.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("timer subsystem")
class TimerSubsystemTest {

    @Mock
    private DistributedTimerService timerService;

    @Mock
    private TableSnapshotRepository repository;

    private MutableClock clock;
    private SlaTimerFiringBus bus;
    private List<SlaTimerFiring> firings;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(Fixtures.T0);
        bus = new SlaTimerFiringBus();
        firings = new CopyOnWriteArrayList<>();
        bus.firings().doOnNext(firings::add).subscribe();
    }

    // ------------------------------------------------------------------ //

    @Nested
    @DisplayName("SlaTimerFiring")
    class FiringContract {

        @Test
        @DisplayName("defends its mandatory components")
        void defendsContracts() {
            assertThatNullPointerException().isThrownBy(() -> new SlaTimerFiring(
                    null, SlaKind.WAITER_RESPONSE, Fixtures.T0, FiringSource.KEYSPACE_EXPIRY));
            assertThatNullPointerException().isThrownBy(() -> new SlaTimerFiring(
                    "T-01", null, Fixtures.T0, FiringSource.KEYSPACE_EXPIRY));
            assertThatNullPointerException().isThrownBy(() -> new SlaTimerFiring(
                    "T-01", SlaKind.WAITER_RESPONSE, null, FiringSource.KEYSPACE_EXPIRY));
            assertThatNullPointerException().isThrownBy(() -> new SlaTimerFiring(
                    "T-01", SlaKind.WAITER_RESPONSE, Fixtures.T0, null));
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> new SlaTimerFiring(" ", SlaKind.WAITER_RESPONSE,
                            Fixtures.T0, FiringSource.KEYSPACE_EXPIRY))
                    .withMessageContaining("blank");
        }
    }

    @Nested
    @DisplayName("SlaTimerFiringBus")
    class FiringBus {

        @Test
        @DisplayName("fans a claimed firing out to its subscriber")
        void publishesToSubscriber() {
            SlaTimerFiring firing = new SlaTimerFiring("T-01", SlaKind.WAITER_RESPONSE,
                    Fixtures.T0, FiringSource.KEYSPACE_EXPIRY);

            assertThat(bus.publish(firing)).isTrue();
            assertThat(firings).containsExactly(firing);
        }

        @Test
        @DisplayName("rejects a null firing")
        void rejectsNull() {
            assertThatNullPointerException().isThrownBy(() -> bus.publish(null));
        }

        @Test
        @DisplayName("exposes a hot stream that survives a subscriber coming and going")
        void streamSurvivesSubscriberChurn() {
            SlaTimerFiringBus fresh = new SlaTimerFiringBus();
            var first = fresh.firings().subscribe();
            first.dispose();

            List<SlaTimerFiring> second = new CopyOnWriteArrayList<>();
            fresh.firings().doOnNext(second::add).subscribe();
            SlaTimerFiring firing = new SlaTimerFiring("T-02", SlaKind.TABLE_TURNAROUND,
                    Fixtures.T0, FiringSource.RECONCILIATION_SWEEP);

            assertThat(fresh.publish(firing)).isTrue();
            assertThat(second).containsExactly(firing);
        }
    }

    @Nested
    @DisplayName("SlaReconciliationSweeper")
    class Sweeper {

        private SlaReconciliationSweeper sweeper;

        @BeforeEach
        void createSweeper() {
            sweeper = new SlaReconciliationSweeper(
                    timerService, bus, TestProperties.defaults(), clock);
        }

        @Test
        @DisplayName("claims every overdue deadline and publishes each as a sweep firing")
        void claimsOverdueDeadlines() {
            Mockito.when(timerService.findOverdue(eq(Fixtures.T0), anyInt()))
                    .thenReturn(Flux.just("T-01", "T-02"));
            Mockito.when(timerService.claim("T-01", Fixtures.T0))
                    .thenReturn(Mono.just(SlaKind.WAITER_RESPONSE));
            Mockito.when(timerService.claim("T-02", Fixtures.T0))
                    .thenReturn(Mono.just(SlaKind.TABLE_TURNAROUND));

            StepVerifier.create(sweeper.sweepOnce()).expectNext(2L).verifyComplete();

            assertThat(firings).hasSize(2);
            assertThat(firings).allSatisfy(firing -> assertThat(firing.source())
                    .isEqualTo(FiringSource.RECONCILIATION_SWEEP));
            assertThat(firings.get(0).slaKind()).isEqualTo(SlaKind.WAITER_RESPONSE);
            assertThat(firings.get(1).slaKind()).isEqualTo(SlaKind.TABLE_TURNAROUND);
            assertThat(sweeper.sweepCount()).isEqualTo(1L);
            assertThat(sweeper.claimCount()).isEqualTo(2L);
        }

        @Test
        @DisplayName("publishes nothing for a deadline another instance has already claimed")
        void skipsAlreadyClaimedDeadlines() {
            Mockito.when(timerService.findOverdue(eq(Fixtures.T0), anyInt()))
                    .thenReturn(Flux.just("T-01"));
            Mockito.when(timerService.claim("T-01", Fixtures.T0)).thenReturn(Mono.empty());

            StepVerifier.create(sweeper.sweepOnce()).expectNext(0L).verifyComplete();

            assertThat(firings)
                    .as("exactly one instance in the cluster alerts, and it was not this one")
                    .isEmpty();
            assertThat(sweeper.claimCount()).isZero();
        }

        @Test
        @DisplayName("continues sweeping after one table's claim fails")
        void isolatesPerTableFailures() {
            Mockito.when(timerService.findOverdue(eq(Fixtures.T0), anyInt()))
                    .thenReturn(Flux.just("T-01", "T-02"));
            Mockito.when(timerService.claim("T-01", Fixtures.T0))
                    .thenReturn(Mono.error(new IllegalStateException("redis blip")));
            Mockito.when(timerService.claim("T-02", Fixtures.T0))
                    .thenReturn(Mono.just(SlaKind.WAITER_RESPONSE));

            StepVerifier.create(sweeper.sweepOnce()).expectNext(1L).verifyComplete();

            assertThat(firings).hasSize(1);
            assertThat(firings.get(0).tableId()).isEqualTo("T-02");
        }

        @Test
        @DisplayName("finds nothing to do when no deadline is overdue")
        void quietSweep() {
            Mockito.when(timerService.findOverdue(eq(Fixtures.T0), anyInt()))
                    .thenReturn(Flux.empty());

            StepVerifier.create(sweeper.sweepOnce()).expectNext(0L).verifyComplete();
            assertThat(firings).isEmpty();
        }

        @Test
        @DisplayName("advances its reference instant with the clock")
        void usesTheInjectedClock() {
            Instant later = Fixtures.T0.plus(Duration.ofMinutes(7));
            clock.set(later);
            Mockito.when(timerService.findOverdue(eq(later), anyInt())).thenReturn(Flux.empty());

            StepVerifier.create(sweeper.sweepOnce()).expectNext(0L).verifyComplete();

            Mockito.verify(timerService).findOverdue(eq(later), anyInt());
        }

        @Test
        @DisplayName("does not start when disabled by configuration")
        void respectsDisabledFlag() {
            RestoStreamProperties.Timers timers = TestProperties.defaultTimers();
            RestoStreamProperties disabled = TestProperties.withTimers(
                    new RestoStreamProperties.Timers(timers.keyspaceNotificationsEnabled(),
                            timers.keyspacePattern(), timers.sweepInterval(),
                            timers.sweepBatchSize(), false, timers.countdownEnabled(),
                            timers.countdownInterval(), timers.driftReconcileEnabled(),
                            timers.driftReconcileInterval()));
            SlaReconciliationSweeper quiet =
                    new SlaReconciliationSweeper(timerService, bus, disabled, clock);

            quiet.start();

            assertThat(quiet.isRunning()).isFalse();
            quiet.stop();
            Mockito.verifyNoInteractions(timerService);
        }

        @Test
        @DisplayName("is idempotent across repeated start and stop")
        void startAndStopAreIdempotent() {
            sweeper.start();
            assertThat(sweeper.isRunning()).isTrue();
            sweeper.start();
            assertThat(sweeper.isRunning()).isTrue();

            sweeper.stop();
            assertThat(sweeper.isRunning()).isFalse();
            sweeper.stop();
            assertThat(sweeper.isRunning()).isFalse();
            assertThat(sweeper.getPhase()).isLessThan(Integer.MAX_VALUE);
        }

        @Test
        @DisplayName("defends its construction contract")
        void defendsContracts() {
            RestoStreamProperties properties = TestProperties.defaults();
            assertThatNullPointerException().isThrownBy(() ->
                    new SlaReconciliationSweeper(null, bus, properties, clock));
            assertThatNullPointerException().isThrownBy(() ->
                    new SlaReconciliationSweeper(timerService, null, properties, clock));
            assertThatNullPointerException().isThrownBy(() ->
                    new SlaReconciliationSweeper(timerService, bus, null, clock));
            assertThatNullPointerException().isThrownBy(() ->
                    new SlaReconciliationSweeper(timerService, bus, properties, null));
        }
    }

    @Nested
    @DisplayName("TimerDriftReconciler")
    class DriftReconciler {

        private TimerDriftReconciler reconciler;

        @BeforeEach
        void createReconciler() {
            reconciler = new TimerDriftReconciler(
                    repository, timerService, TestProperties.defaults(), clock);
        }

        @Test
        @DisplayName("re-arms a snapshot that claims an armed SLA with no indexed deadline")
        void reArmsMissingDeadline() {
            TableSnapshot seated = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);
            clock.set(Fixtures.T0.plus(Duration.ofMinutes(2)));
            Mockito.when(repository.findAll()).thenReturn(Flux.just(seated));
            Mockito.when(timerService.deadlineOf(Fixtures.TABLE_ID)).thenReturn(Mono.empty());
            Mockito.when(timerService.arm(any(), any(), any(), any())).thenReturn(Mono.empty());

            StepVerifier.create(reconciler.reconcileOnce())
                    .assertNext(report -> {
                        assertThat(report.inspected()).isEqualTo(1L);
                        assertThat(report.rearmed()).isEqualTo(1L);
                        assertThat(report.cancelled()).isZero();
                        assertThat(report.repaired()).isEqualTo(1L);
                    })
                    .verifyComplete();

            Mockito.verify(timerService).arm(Fixtures.TABLE_ID, SlaKind.WAITER_RESPONSE,
                    Duration.ofMinutes(3), seated.slaDeadline());
        }

        @Test
        @DisplayName("re-arms an already-overdue deadline with a minimal window so it fires at once")
        void reArmsOverdueDeadlineImmediately() {
            TableSnapshot seated = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);
            clock.set(seated.slaDeadline().plus(Duration.ofMinutes(1)));
            Mockito.when(repository.findAll()).thenReturn(Flux.just(seated));
            Mockito.when(timerService.deadlineOf(Fixtures.TABLE_ID)).thenReturn(Mono.empty());
            Mockito.when(timerService.arm(any(), any(), any(), any())).thenReturn(Mono.empty());

            StepVerifier.create(reconciler.reconcileOnce())
                    .assertNext(report -> assertThat(report.rearmed()).isEqualTo(1L))
                    .verifyComplete();

            Mockito.verify(timerService).arm(Fixtures.TABLE_ID, SlaKind.WAITER_RESPONSE,
                    Duration.ofMillis(1), seated.slaDeadline());
        }

        @Test
        @DisplayName("cancels an orphaned deadline left behind for an unguarded table")
        void cancelsOrphanedDeadline() {
            TableSnapshot free = Fixtures.freeTable();
            Mockito.when(repository.findAll()).thenReturn(Flux.just(free));
            Mockito.when(timerService.deadlineOf(Fixtures.TABLE_ID))
                    .thenReturn(Mono.just(Fixtures.T0.plus(Fixtures.WAITER_SLA)));
            Mockito.when(timerService.cancel(Fixtures.TABLE_ID)).thenReturn(Mono.just(true));

            StepVerifier.create(reconciler.reconcileOnce())
                    .assertNext(report -> {
                        assertThat(report.cancelled()).isEqualTo(1L);
                        assertThat(report.rearmed()).isZero();
                    })
                    .verifyComplete();

            Mockito.verify(timerService).cancel(Fixtures.TABLE_ID);
        }

        @Test
        @DisplayName("leaves an already-alerted breach alone")
        void leavesBreachedTablesAlone() {
            TableSnapshot breached = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA)
                    .breached(Fixtures.T0.plus(Fixtures.WAITER_SLA));
            Mockito.when(repository.findAll()).thenReturn(Flux.just(breached));
            Mockito.when(timerService.deadlineOf(Fixtures.TABLE_ID)).thenReturn(Mono.empty());

            StepVerifier.create(reconciler.reconcileOnce())
                    .assertNext(report -> assertThat(report.repaired())
                            .as("a breached table has already been alerted; re-arming would double-page")
                            .isZero())
                    .verifyComplete();

            Mockito.verify(timerService, Mockito.never()).arm(any(), any(), any(), any());
        }

        @Test
        @DisplayName("reports no drift when snapshots and the deadline index agree")
        void reportsNoDriftWhenConsistent() {
            TableSnapshot seated = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);
            TableSnapshot free = TableSnapshot.provision("T-02", "MAIN", 4, Fixtures.T0);
            Mockito.when(repository.findAll()).thenReturn(Flux.just(seated, free));
            Mockito.when(timerService.deadlineOf(Fixtures.TABLE_ID))
                    .thenReturn(Mono.just(seated.slaDeadline()));
            Mockito.when(timerService.deadlineOf("T-02")).thenReturn(Mono.empty());

            StepVerifier.create(reconciler.reconcileOnce())
                    .assertNext(report -> {
                        assertThat(report.inspected()).isEqualTo(2L);
                        assertThat(report.repaired()).isZero();
                    })
                    .verifyComplete();

            Mockito.verify(timerService, Mockito.never()).arm(any(), any(), any(), any());
            Mockito.verify(timerService, Mockito.never()).cancel(any());
        }

        @Test
        @DisplayName("walks an empty floor plan without complaint")
        void handlesEmptyFloorPlan() {
            Mockito.when(repository.findAll()).thenReturn(Flux.empty());

            StepVerifier.create(reconciler.reconcileOnce())
                    .assertNext(report -> assertThat(report.inspected()).isZero())
                    .verifyComplete();
        }

        @Test
        @DisplayName("repairs drift in both directions in a single pass")
        void repairsBothDirections() {
            TableSnapshot missingDeadline = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);
            TableSnapshot orphanedDeadline = new TableSnapshot("T-02", "MAIN", 4,
                    TableState.FREE, Fixtures.T0, 5L, null, null, false, 0,
                    null, null, null, Fixtures.T0);
            Mockito.when(repository.findAll())
                    .thenReturn(Flux.just(missingDeadline, orphanedDeadline));
            Mockito.when(timerService.deadlineOf(Fixtures.TABLE_ID)).thenReturn(Mono.empty());
            Mockito.when(timerService.deadlineOf("T-02"))
                    .thenReturn(Mono.just(Fixtures.T0.plus(Duration.ofMinutes(9))));
            Mockito.when(timerService.arm(any(), any(), any(), any())).thenReturn(Mono.empty());
            Mockito.when(timerService.cancel("T-02")).thenReturn(Mono.just(true));

            StepVerifier.create(reconciler.reconcileOnce())
                    .assertNext(report -> {
                        assertThat(report.inspected()).isEqualTo(2L);
                        assertThat(report.rearmed()).isEqualTo(1L);
                        assertThat(report.cancelled()).isEqualTo(1L);
                        assertThat(report.repaired()).isEqualTo(2L);
                    })
                    .verifyComplete();
        }

        @Test
        @DisplayName("does not start when disabled by configuration")
        void respectsDisabledFlag() {
            RestoStreamProperties.Timers timers = TestProperties.defaultTimers();
            RestoStreamProperties disabled = TestProperties.withTimers(
                    new RestoStreamProperties.Timers(timers.keyspaceNotificationsEnabled(),
                            timers.keyspacePattern(), timers.sweepInterval(),
                            timers.sweepBatchSize(), timers.sweepEnabled(),
                            timers.countdownEnabled(), timers.countdownInterval(),
                            false, timers.driftReconcileInterval()));
            TimerDriftReconciler quiet =
                    new TimerDriftReconciler(repository, timerService, disabled, clock);

            quiet.start();

            assertThat(quiet.isRunning()).isFalse();
            quiet.stop();
            Mockito.verifyNoInteractions(repository, timerService);
        }

        @Test
        @DisplayName("is idempotent across repeated start and stop")
        void startAndStopAreIdempotent() {
            reconciler.start();
            assertThat(reconciler.isRunning()).isTrue();
            reconciler.start();
            reconciler.stop();
            assertThat(reconciler.isRunning()).isFalse();
            reconciler.stop();
            assertThat(reconciler.getPhase()).isLessThan(Integer.MAX_VALUE);
        }

        @Test
        @DisplayName("defends its construction contract")
        void defendsContracts() {
            RestoStreamProperties properties = TestProperties.defaults();
            assertThatNullPointerException().isThrownBy(() ->
                    new TimerDriftReconciler(null, timerService, properties, clock));
            assertThatNullPointerException().isThrownBy(() ->
                    new TimerDriftReconciler(repository, null, properties, clock));
            assertThatNullPointerException().isThrownBy(() ->
                    new TimerDriftReconciler(repository, timerService, null, clock));
            assertThatNullPointerException().isThrownBy(() ->
                    new TimerDriftReconciler(repository, timerService, properties, null));
        }
    }

    @Nested
    @DisplayName("FiringSource")
    class FiringSourceContract {

        @Test
        @DisplayName("names both independent detection paths")
        void namesBothPaths() {
            assertThat(FiringSource.values())
                    .containsExactly(FiringSource.KEYSPACE_EXPIRY,
                            FiringSource.RECONCILIATION_SWEEP);
        }
    }
}
