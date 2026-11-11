/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.broadcast;

import com.satyendra.restostream.alert.AlertSeverity;
import com.satyendra.restostream.alert.SlaAlert;
import com.satyendra.restostream.config.RestoStreamProperties;
import com.satyendra.restostream.domain.SlaKind;
import com.satyendra.restostream.domain.TableSnapshot;
import com.satyendra.restostream.domain.TableState;
import com.satyendra.restostream.repository.TableSnapshotRepository;
import com.satyendra.restostream.support.Fixtures;
import com.satyendra.restostream.support.MutableClock;
import com.satyendra.restostream.support.TestProperties;
import com.satyendra.restostream.timer.FiringSource;
import com.satyendra.restostream.timer.SlaTimerFiring;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Unit suite for the outbound half of the pipeline: the transport-agnostic fan-out, the live
 * countdown publisher, and the SLA alert payload.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("broadcast and alerting")
class BroadcastAndAlertTest {

    @Mock
    private TableSnapshotRepository repository;

    private MutableClock clock;
    private DashboardBroadcaster broadcaster;
    private List<BroadcastEnvelope> envelopes;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(Fixtures.T0);
        broadcaster = new DashboardBroadcaster(
                TestProperties.defaults(), new SimpleMeterRegistry(), clock);
        envelopes = new CopyOnWriteArrayList<>();
        broadcaster.envelopes().doOnNext(envelopes::add).subscribe();
    }

    // ------------------------------------------------------------------ //

    @Nested
    @DisplayName("DashboardBroadcaster")
    class Broadcaster {

        @Test
        @DisplayName("addresses a table state change to both the floor feed and the table's own feed")
        void addressesTableStateToBothFeeds() {
            TableSnapshot seated = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);

            broadcaster.broadcastTableState(seated);

            assertThat(envelopes).hasSize(1);
            BroadcastEnvelope envelope = envelopes.get(0);
            assertThat(envelope.kind()).isEqualTo(BroadcastKind.TABLE_STATE);
            assertThat(envelope.destinations())
                    .containsExactly("/topic/tables", "/topic/tables/T-01");
            assertThat(envelope.payload()).isSameAs(seated);
            assertThat(envelope.emittedAt()).isEqualTo(Fixtures.T0);
        }

        @Test
        @DisplayName("addresses the floor snapshot to the aggregate feed only")
        void addressesFloorSnapshot() {
            List<TableSnapshot> floor = List.of(
                    Fixtures.freeTable(), TableSnapshot.provision("T-02", "MAIN", 4, Fixtures.T0));

            broadcaster.broadcastFloorSnapshot(floor);

            assertThat(envelopes.get(0).kind()).isEqualTo(BroadcastKind.FLOOR_SNAPSHOT);
            assertThat(envelopes.get(0).destinations()).containsExactly("/topic/tables");
            assertThat((List<?>) envelopes.get(0).payload()).hasSize(2);
        }

        @Test
        @DisplayName("addresses an SLA alert to the alert feed and the breaching table's feed")
        void addressesSlaAlert() {
            SlaAlert alert = SlaAlert.from(
                    Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA)
                            .breached(Fixtures.T0.plus(Fixtures.WAITER_SLA)),
                    new SlaTimerFiring(Fixtures.TABLE_ID, SlaKind.WAITER_RESPONSE,
                            Fixtures.T0.plus(Fixtures.WAITER_SLA), FiringSource.KEYSPACE_EXPIRY));

            broadcaster.broadcastSlaAlert(alert);

            assertThat(envelopes.get(0).kind()).isEqualTo(BroadcastKind.SLA_ALERT);
            assertThat(envelopes.get(0).destinations())
                    .containsExactly("/topic/sla-alerts", "/topic/tables/T-01");
        }

        @Test
        @DisplayName("publishes countdown ticks to the timer feed and nothing when there are none")
        void addressesTimerTicks() {
            TimerTick tick = TimerTick.from(
                    Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA), Fixtures.T0);

            broadcaster.broadcastTimerTicks(List.of());
            assertThat(envelopes)
                    .as("an empty restaurant must generate no traffic at all")
                    .isEmpty();

            broadcaster.broadcastTimerTicks(List.of(tick));

            assertThat(envelopes).hasSize(1);
            assertThat(envelopes.get(0).kind()).isEqualTo(BroadcastKind.TIMER_TICK);
            assertThat(envelopes.get(0).destinations()).containsExactly("/topic/timers");
        }

        @Test
        @DisplayName("defends its contracts")
        void defendsContracts() {
            assertThatNullPointerException().isThrownBy(() -> broadcaster.broadcastTableState(null));
            assertThatNullPointerException().isThrownBy(() -> broadcaster.broadcastFloorSnapshot(null));
            assertThatNullPointerException().isThrownBy(() -> broadcaster.broadcastSlaAlert(null));
            assertThatNullPointerException().isThrownBy(() -> broadcaster.broadcastTimerTicks(null));
            assertThatNullPointerException().isThrownBy(() -> new DashboardBroadcaster(
                    null, new SimpleMeterRegistry(), clock));
            assertThatNullPointerException().isThrownBy(() -> new DashboardBroadcaster(
                    TestProperties.defaults(), null, clock));
            assertThatNullPointerException().isThrownBy(() -> new DashboardBroadcaster(
                    TestProperties.defaults(), new SimpleMeterRegistry(), null));
        }
    }

    @Nested
    @DisplayName("BroadcastEnvelope")
    class Envelope {

        @Test
        @DisplayName("requires at least one destination and copies the list it is given")
        void defendsContracts() {
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> new BroadcastEnvelope(List.of(),
                            BroadcastKind.TABLE_STATE, "payload", Fixtures.T0))
                    .withMessageContaining("must not be empty");
            assertThatNullPointerException().isThrownBy(() -> new BroadcastEnvelope(
                    null, BroadcastKind.TABLE_STATE, "payload", Fixtures.T0));
            assertThatNullPointerException().isThrownBy(() -> new BroadcastEnvelope(
                    List.of("/topic/x"), null, "payload", Fixtures.T0));
            assertThatNullPointerException().isThrownBy(() -> new BroadcastEnvelope(
                    List.of("/topic/x"), BroadcastKind.TABLE_STATE, null, Fixtures.T0));
            assertThatNullPointerException().isThrownBy(() -> new BroadcastEnvelope(
                    List.of("/topic/x"), BroadcastKind.TABLE_STATE, "payload", null));

            BroadcastEnvelope envelope = new BroadcastEnvelope(
                    new java.util.ArrayList<>(List.of("/topic/x")),
                    BroadcastKind.TABLE_STATE, "payload", Fixtures.T0);
            assertThatExceptionOfType(UnsupportedOperationException.class)
                    .isThrownBy(() -> envelope.destinations().add("/topic/y"));
        }
    }

    @Nested
    @DisplayName("TimerTick")
    class Tick {

        @Test
        @DisplayName("renders the remaining window for an armed table")
        void rendersRemainingWindow() {
            TableSnapshot seated = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);
            Instant at = Fixtures.T0.plus(Duration.ofMinutes(2));

            TimerTick tick = TimerTick.from(seated, at);

            assertThat(tick.tableId()).isEqualTo(Fixtures.TABLE_ID);
            assertThat(tick.slaKind()).isEqualTo(SlaKind.WAITER_RESPONSE);
            assertThat(tick.deadline()).isEqualTo(seated.slaDeadline());
            assertThat(tick.remainingMillis()).isEqualTo(Duration.ofMinutes(3).toMillis());
            assertThat(tick.breached()).isFalse();
            assertThat(tick.emittedAt()).isEqualTo(at);
        }

        @Test
        @DisplayName("floors the remaining window at zero once the deadline has passed")
        void floorsAtZero() {
            TableSnapshot seated = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);

            TimerTick tick = TimerTick.from(seated, Fixtures.T0.plus(Duration.ofHours(1)));

            assertThat(tick.remainingMillis()).isZero();
        }

        @Test
        @DisplayName("refuses to tick a table carrying no armed SLA")
        void refusesUnarmedTable() {
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> TimerTick.from(Fixtures.freeTable(), Fixtures.T0))
                    .withMessageContaining("carries no armed SLA");
            assertThatNullPointerException()
                    .isThrownBy(() -> TimerTick.from(null, Fixtures.T0));
            assertThatNullPointerException().isThrownBy(() -> TimerTick.from(
                    Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA), null));
            assertThatNullPointerException().isThrownBy(() -> new TimerTick(
                    null, SlaKind.WAITER_RESPONSE, Fixtures.T0, 0L, false, Fixtures.T0));
        }
    }

    @Nested
    @DisplayName("SlaCountdownPublisher")
    class Countdown {

        private SlaCountdownPublisher publisher;

        @BeforeEach
        void createPublisher() {
            publisher = new SlaCountdownPublisher(
                    repository, broadcaster, TestProperties.defaults(), clock);
        }

        @Test
        @DisplayName("publishes only armed tables, soonest deadline first")
        void publishesArmedTablesInUrgencyOrder() {
            TableSnapshot soon = new TableSnapshot("T-02", "MAIN", 4, TableState.SEATED,
                    Fixtures.T0, 1L, SlaKind.WAITER_RESPONSE,
                    Fixtures.T0.plus(Duration.ofMinutes(1)), false, 2,
                    null, null, null, Fixtures.T0);
            TableSnapshot later = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);
            Mockito.when(repository.findAll())
                    .thenReturn(Flux.just(later, soon, Fixtures.freeTable()));

            StepVerifier.create(publisher.publishOnce()).expectNext(2).verifyComplete();

            assertThat(envelopes).hasSize(1);
            @SuppressWarnings("unchecked")
            List<TimerTick> ticks = (List<TimerTick>) envelopes.get(0).payload();
            assertThat(ticks).extracting(TimerTick::tableId).containsExactly("T-02", "T-01");
        }

        @Test
        @DisplayName("broadcasts nothing when no table is armed")
        void quietWhenNothingArmed() {
            Mockito.when(repository.findAll()).thenReturn(Flux.just(Fixtures.freeTable()));

            StepVerifier.create(publisher.publishOnce()).expectNext(0).verifyComplete();

            assertThat(envelopes).isEmpty();
        }

        @Test
        @DisplayName("does not start when disabled by configuration")
        void respectsDisabledFlag() {
            RestoStreamProperties.Timers timers = TestProperties.defaultTimers();
            RestoStreamProperties disabled = TestProperties.withTimers(
                    new RestoStreamProperties.Timers(timers.keyspaceNotificationsEnabled(),
                            timers.keyspacePattern(), timers.sweepInterval(),
                            timers.sweepBatchSize(), timers.sweepEnabled(), false,
                            timers.countdownInterval(), timers.driftReconcileEnabled(),
                            timers.driftReconcileInterval()));
            SlaCountdownPublisher quiet =
                    new SlaCountdownPublisher(repository, broadcaster, disabled, clock);

            quiet.start();

            assertThat(quiet.isRunning()).isFalse();
            quiet.stop();
            Mockito.verifyNoInteractions(repository);
        }

        @Test
        @DisplayName("is idempotent across repeated start and stop")
        void startAndStopAreIdempotent() {
            publisher.start();
            assertThat(publisher.isRunning()).isTrue();
            publisher.start();
            publisher.stop();
            assertThat(publisher.isRunning()).isFalse();
            publisher.stop();
            assertThat(publisher.getPhase()).isLessThan(Integer.MAX_VALUE);
        }

        @Test
        @DisplayName("defends its construction contract")
        void defendsContracts() {
            RestoStreamProperties properties = TestProperties.defaults();
            assertThatNullPointerException().isThrownBy(() ->
                    new SlaCountdownPublisher(null, broadcaster, properties, clock));
            assertThatNullPointerException().isThrownBy(() ->
                    new SlaCountdownPublisher(repository, null, properties, clock));
            assertThatNullPointerException().isThrownBy(() ->
                    new SlaCountdownPublisher(repository, broadcaster, null, clock));
            assertThatNullPointerException().isThrownBy(() ->
                    new SlaCountdownPublisher(repository, broadcaster, properties, null));
        }
    }

    @Nested
    @DisplayName("SlaAlert")
    class Alert {

        @Test
        @DisplayName("summarises a waiter-response breach as CRITICAL with a readable message")
        void summarisesWaiterBreach() {
            TableSnapshot seated = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);
            Instant detectedAt = seated.slaDeadline().plus(Duration.ofSeconds(3));
            SlaAlert alert = SlaAlert.from(seated.breached(detectedAt),
                    new SlaTimerFiring(Fixtures.TABLE_ID, SlaKind.WAITER_RESPONSE,
                            detectedAt, FiringSource.KEYSPACE_EXPIRY));

            assertThat(alert.alertId()).isNotBlank();
            assertThat(alert.tableId()).isEqualTo(Fixtures.TABLE_ID);
            assertThat(alert.zone()).isEqualTo(Fixtures.ZONE);
            assertThat(alert.severity()).isEqualTo(AlertSeverity.CRITICAL);
            assertThat(alert.tableState()).isEqualTo(TableState.SEATED);
            assertThat(alert.dueAt()).isEqualTo(seated.slaDeadline());
            assertThat(alert.detectedAt()).isEqualTo(detectedAt);
            assertThat(alert.overdueMillis()).isEqualTo(3_000L);
            assertThat(alert.dwellMillis()).isEqualTo(Duration.ofMinutes(5).toMillis() + 3_000L);
            assertThat(alert.occupancy()).isEqualTo(2);
            assertThat(alert.detectedBy()).isEqualTo(FiringSource.KEYSPACE_EXPIRY);
            assertThat(alert.message())
                    .contains("T-01")
                    .contains("WINDOW")
                    .contains("no waiter response")
                    .contains("5 min 03 s");
        }

        @Test
        @DisplayName("summarises a turnaround breach as a WARNING about lost revenue")
        void summarisesTurnaroundBreach() {
            TableSnapshot uncleaned = Fixtures.uncleanedTable(Fixtures.T0, Fixtures.TURNAROUND_SLA);
            Instant detectedAt = uncleaned.slaDeadline();
            SlaAlert alert = SlaAlert.from(uncleaned.breached(detectedAt),
                    new SlaTimerFiring(Fixtures.TABLE_ID, SlaKind.TABLE_TURNAROUND,
                            detectedAt, FiringSource.RECONCILIATION_SWEEP));

            assertThat(alert.severity()).isEqualTo(AlertSeverity.WARNING);
            assertThat(alert.tableState()).isEqualTo(TableState.UNCLEANED);
            assertThat(alert.overdueMillis()).isZero();
            assertThat(alert.message()).contains("awaiting cleaning");
        }

        @Test
        @DisplayName("floors a negative overdue interval at zero")
        void floorsNegativeOverdue() {
            TableSnapshot seated = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);
            Instant early = seated.slaDeadline().minus(Duration.ofSeconds(10));

            SlaAlert alert = SlaAlert.from(seated.breached(early),
                    new SlaTimerFiring(Fixtures.TABLE_ID, SlaKind.WAITER_RESPONSE,
                            early, FiringSource.KEYSPACE_EXPIRY));

            assertThat(alert.overdueMillis()).isZero();
        }

        @ParameterizedTest
        @EnumSource(SlaKind.class)
        @DisplayName("maps every SLA class to a severity")
        void mapsEverySlaToASeverity(SlaKind kind) {
            assertThat(AlertSeverity.forSla(kind)).isNotNull();
        }

        @Test
        @DisplayName("defends its mandatory components")
        void defendsContracts() {
            assertThatNullPointerException().isThrownBy(() -> SlaAlert.from(null,
                    new SlaTimerFiring("T-01", SlaKind.WAITER_RESPONSE,
                            Fixtures.T0, FiringSource.KEYSPACE_EXPIRY)));
            assertThatNullPointerException().isThrownBy(() ->
                    SlaAlert.from(Fixtures.freeTable(), null));
            assertThatNullPointerException().isThrownBy(() -> new SlaAlert(
                    null, "T-01", "Z", SlaKind.WAITER_RESPONSE, AlertSeverity.CRITICAL,
                    TableState.SEATED, Fixtures.T0, Fixtures.T0, 0L, 0L, 0,
                    FiringSource.KEYSPACE_EXPIRY, "m"));
            assertThatNullPointerException().isThrownBy(() -> new SlaAlert(
                    "a", "T-01", "Z", SlaKind.WAITER_RESPONSE, AlertSeverity.CRITICAL,
                    TableState.SEATED, Fixtures.T0, Fixtures.T0, 0L, 0L, 0,
                    FiringSource.KEYSPACE_EXPIRY, null));
        }
    }
}
