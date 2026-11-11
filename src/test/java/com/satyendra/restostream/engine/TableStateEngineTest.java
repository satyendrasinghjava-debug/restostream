/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.engine;

import com.satyendra.restostream.domain.OccupancySignal;
import com.satyendra.restostream.domain.SignalType;
import com.satyendra.restostream.domain.SlaKind;
import com.satyendra.restostream.domain.TableSnapshot;
import com.satyendra.restostream.domain.TableState;
import com.satyendra.restostream.support.Fixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Exhaustive unit suite for the Table State Engine.</p>
 *
 * <p>Because the engine is a pure function of {@code (snapshot, signal, instant)}, every transition,
 * every guard and every timer-race outcome can be asserted with plain values — no Redis, no event
 * loop, no sleeping, and no mock of a network client anywhere in this file. The one collaborator,
 * {@link SlaPolicy}, is a single-method interface and is supplied either as a lambda or as a
 * Mockito stub where the interaction itself is the thing under test.</p>
 *
 * <p>The suite is structured to make the 90% line and branch gate on this package a real
 * measurement: the full 3-state × 5-signal decision matrix is swept parameterically, and each guard
 * is additionally asserted individually so a regression names itself.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@DisplayName("TableStateEngine")
class TableStateEngineTest {

    private final TableStateEngine engine = new TableStateEngine(Fixtures.defaultPolicy());

    // ------------------------------------------------------------------ //
    // Construction and argument contracts
    // ------------------------------------------------------------------ //

    @Nested
    @DisplayName("argument contracts")
    class ArgumentContracts {

        @Test
        @DisplayName("rejects a null SLA policy at construction")
        void rejectsNullPolicy() {
            assertThatNullPointerException()
                    .isThrownBy(() -> new TableStateEngine(null))
                    .withMessageContaining("slaPolicy");
        }

        @Test
        @DisplayName("rejects a null current snapshot")
        void rejectsNullCurrent() {
            assertThatNullPointerException()
                    .isThrownBy(() -> engine.decide(
                            null, Fixtures.signal(SignalType.GUEST_SEATED, Fixtures.T0), Fixtures.T0))
                    .withMessageContaining("current");
        }

        @Test
        @DisplayName("rejects a null signal")
        void rejectsNullSignal() {
            assertThatNullPointerException()
                    .isThrownBy(() -> engine.decide(Fixtures.freeTable(), null, Fixtures.T0))
                    .withMessageContaining("signal");
        }

        @Test
        @DisplayName("rejects a null instant")
        void rejectsNullInstant() {
            assertThatNullPointerException()
                    .isThrownBy(() -> engine.decide(Fixtures.freeTable(),
                            Fixtures.signal(SignalType.GUEST_SEATED, Fixtures.T0), null))
                    .withMessageContaining("now");
        }

        @Test
        @DisplayName("refuses to evaluate a signal raised against a different table")
        void refusesMismatchedTable() {
            OccupancySignal otherTable =
                    Fixtures.signal("T-99", SignalType.GUEST_SEATED, Fixtures.T0, 2);

            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> engine.decide(Fixtures.freeTable(), otherTable, Fixtures.T0))
                    .withMessageContaining("T-99")
                    .withMessageContaining(Fixtures.TABLE_ID);
        }
    }

    // ------------------------------------------------------------------ //
    // The happy-path lifecycle
    // ------------------------------------------------------------------ //

    @Nested
    @DisplayName("lifecycle transitions")
    class Lifecycle {

        @Test
        @DisplayName("FREE + GUEST_SEATED advances to SEATED and arms the five-minute waiter SLA")
        void seatsAFreeTable() {
            TableSnapshot free = Fixtures.freeTable();
            OccupancySignal seated =
                    Fixtures.signal(Fixtures.TABLE_ID, SignalType.GUEST_SEATED, Fixtures.T0, 3);

            TransitionDecision decision = engine.decide(free, seated, Fixtures.T0);

            assertThat(decision).isInstanceOf(TransitionDecision.Applied.class);
            TransitionDecision.Applied applied = (TransitionDecision.Applied) decision;

            assertThat(applied.isApplied()).isTrue();
            assertThat(applied.current()).isSameAs(free);
            assertThat(applied.next().state()).isEqualTo(TableState.SEATED);
            assertThat(applied.next().colourName()).isEqualTo("RED");
            assertThat(applied.next().version()).isEqualTo(free.version() + 1L);
            assertThat(applied.next().stateSince()).isEqualTo(Fixtures.T0);
            assertThat(applied.next().occupancy()).isEqualTo(3);
            assertThat(applied.next().slaBreached()).isFalse();
            assertThat(applied.next().slaKind()).isEqualTo(SlaKind.WAITER_RESPONSE);
            assertThat(applied.next().slaDeadline()).isEqualTo(Fixtures.T0.plus(Fixtures.WAITER_SLA));
            assertThat(applied.next().slaArmed()).isTrue();
            assertThat(applied.next().slaRemaining(Fixtures.T0)).isEqualTo(Fixtures.WAITER_SLA);

            assertThat(applied.timer().action()).isEqualTo(TimerAction.ARM);
            assertThat(applied.timer().isArm()).isTrue();
            assertThat(applied.timer().slaKind()).isEqualTo(SlaKind.WAITER_RESPONSE);
            assertThat(applied.timer().ttl()).isEqualTo(Fixtures.WAITER_SLA);
            assertThat(applied.timer().deadline()).isEqualTo(Fixtures.T0.plus(Fixtures.WAITER_SLA));

            assertThat(applied.narrative())
                    .contains(Fixtures.TABLE_ID)
                    .contains("FREE -> SEATED")
                    .contains("WAITER_RESPONSE SLA armed");
        }

        @Test
        @DisplayName("SEATED + GUEST_DEPARTED advances to UNCLEANED and arms the turnaround SLA")
        void vacatesASeatedTable() {
            TableSnapshot seated = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);
            Instant departedAt = Fixtures.T0.plus(Duration.ofMinutes(42));

            TransitionDecision.Applied applied = (TransitionDecision.Applied) engine.decide(
                    seated, Fixtures.signal(SignalType.GUEST_DEPARTED, departedAt), departedAt);

            assertThat(applied.next().state()).isEqualTo(TableState.UNCLEANED);
            assertThat(applied.next().colourName()).isEqualTo("AMBER");
            assertThat(applied.next().occupancy())
                    .as("a vacated table holds no guests regardless of what the signal claimed")
                    .isZero();
            assertThat(applied.next().slaKind()).isEqualTo(SlaKind.TABLE_TURNAROUND);
            assertThat(applied.next().slaDeadline())
                    .isEqualTo(departedAt.plus(Fixtures.TURNAROUND_SLA));
            assertThat(applied.timer().action()).isEqualTo(TimerAction.ARM);
            assertThat(applied.next().version()).isEqualTo(seated.version() + 1L);
        }

        @Test
        @DisplayName("UNCLEANED + CLEANING_COMPLETED returns to FREE and cancels every timer")
        void cleansAnUncleanedTable() {
            TableSnapshot uncleaned = Fixtures.uncleanedTable(Fixtures.T0, Fixtures.TURNAROUND_SLA);
            Instant cleanedAt = Fixtures.T0.plus(Duration.ofMinutes(2));

            TransitionDecision.Applied applied = (TransitionDecision.Applied) engine.decide(
                    uncleaned, Fixtures.signal(SignalType.CLEANING_COMPLETED, cleanedAt), cleanedAt);

            assertThat(applied.next().state()).isEqualTo(TableState.FREE);
            assertThat(applied.next().colourName()).isEqualTo("GREEN");
            assertThat(applied.next().slaArmed()).isFalse();
            assertThat(applied.next().slaKind()).isNull();
            assertThat(applied.next().slaDeadline()).isNull();
            assertThat(applied.timer().action()).isEqualTo(TimerAction.CANCEL);
            assertThat(applied.timer().isCancel()).isTrue();
            assertThat(applied.narrative()).contains("any armed SLA timer cancelled");
        }

        @Test
        @DisplayName("FREE + CLEANING_REQUIRED soils a clean table and arms the turnaround SLA")
        void soilsAFreeTable() {
            Instant spilledAt = Fixtures.T0.plus(Duration.ofSeconds(30));

            TransitionDecision.Applied applied = (TransitionDecision.Applied) engine.decide(
                    Fixtures.freeTable(),
                    Fixtures.signal(SignalType.CLEANING_REQUIRED, spilledAt), spilledAt);

            assertThat(applied.next().state()).isEqualTo(TableState.UNCLEANED);
            assertThat(applied.next().slaKind()).isEqualTo(SlaKind.TABLE_TURNAROUND);
        }

        @Test
        @DisplayName("a full FREE -> SEATED -> UNCLEANED -> FREE cycle increments the version each step")
        void completeCycleIsMonotonic() {
            TableSnapshot table = Fixtures.freeTable();
            Instant cursor = Fixtures.T0;

            table = ((TransitionDecision.Applied) engine.decide(
                    table, Fixtures.signal(SignalType.GUEST_SEATED, cursor), cursor)).next();
            assertThat(table.version()).isEqualTo(1L);

            cursor = cursor.plus(Duration.ofMinutes(1));
            table = ((TransitionDecision.Applied) engine.decide(
                    table, Fixtures.signal(SignalType.WAITER_ATTENDED, cursor), cursor)).next();
            assertThat(table.version()).isEqualTo(2L);
            assertThat(table.state()).isEqualTo(TableState.SEATED);

            cursor = cursor.plus(Duration.ofMinutes(50));
            table = ((TransitionDecision.Applied) engine.decide(
                    table, Fixtures.signal(SignalType.GUEST_DEPARTED, cursor), cursor)).next();
            assertThat(table.version()).isEqualTo(3L);

            cursor = cursor.plus(Duration.ofMinutes(2));
            table = ((TransitionDecision.Applied) engine.decide(
                    table, Fixtures.signal(SignalType.CLEANING_COMPLETED, cursor), cursor)).next();

            assertThat(table.version()).isEqualTo(4L);
            assertThat(table.state()).isEqualTo(TableState.FREE);
            assertThat(table.slaArmed()).isFalse();
        }
    }

    // ------------------------------------------------------------------ //
    // Idempotency — the CCTV case
    // ------------------------------------------------------------------ //

    @Nested
    @DisplayName("idempotency")
    class Idempotency {

        @ParameterizedTest(name = "{0} re-asserted against a table already {1} is ignored")
        @CsvSource({
                "GUEST_SEATED,SEATED",
                "GUEST_DEPARTED,UNCLEANED",
                "CLEANING_COMPLETED,FREE",
                "CLEANING_REQUIRED,UNCLEANED"
        })
        @DisplayName("a signal asserting the current state is ignored, not rejected")
        void duplicateAssertionsAreIgnored(SignalType type, TableState state) {
            TableSnapshot table = Fixtures.tableIn(state, Fixtures.T0);

            TransitionDecision decision =
                    engine.decide(table, Fixtures.signal(type, Fixtures.T0), Fixtures.T0);

            assertThat(decision).isInstanceOf(TransitionDecision.Ignored.class);
            assertThat(decision.isApplied()).isFalse();
            assertThat(decision.current()).isSameAs(table);
            assertThat(decision.narrative()).contains("duplicate assertion");
        }

        @Test
        @DisplayName("an ignored decision never changes the version, state or SLA")
        void ignoredDecisionsAreInert() {
            TableSnapshot seated = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);
            Instant later = Fixtures.T0.plus(Duration.ofMinutes(1));

            TransitionDecision.Ignored ignored = (TransitionDecision.Ignored) engine.decide(
                    seated, Fixtures.signal(SignalType.GUEST_SEATED, later), later);

            assertThat(ignored.current().version()).isEqualTo(seated.version());
            assertThat(ignored.current().state()).isEqualTo(seated.state());
            assertThat(ignored.current().slaDeadline())
                    .as("a repeated camera assertion must not push the SLA deadline out")
                    .isEqualTo(seated.slaDeadline());
        }
    }

    // ------------------------------------------------------------------ //
    // Guards
    // ------------------------------------------------------------------ //

    @Nested
    @DisplayName("guards")
    class Guards {

        @Test
        @DisplayName("seating guests at an uncleaned table fails the structural invariant")
        void structuralGuardRejectsNonAdjacentEdge() {
            TableSnapshot uncleaned = Fixtures.uncleanedTable(Fixtures.T0, Fixtures.TURNAROUND_SLA);

            TransitionDecision decision = engine.decide(
                    uncleaned, Fixtures.signal(SignalType.GUEST_SEATED, Fixtures.T0), Fixtures.T0);

            assertThat(decision).isInstanceOf(TransitionDecision.Rejected.class);
            assertThat(decision.narrative())
                    .contains("UNCLEANED -> SEATED")
                    .contains("not part of the table lifecycle");
        }

        @Test
        @DisplayName("a housekeeping alert against a seated table fails operational policy")
        void operationalGuardRejectsWrongSignalForALegalEdge() {
            TableSnapshot seated = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);

            TransitionDecision decision = engine.decide(
                    seated, Fixtures.signal(SignalType.CLEANING_REQUIRED, Fixtures.T0), Fixtures.T0);

            assertThat(decision)
                    .as("SEATED -> UNCLEANED is a legal edge, but only GUEST_DEPARTED may drive it")
                    .isInstanceOf(TransitionDecision.Rejected.class);
            assertThat(decision.narrative())
                    .contains("CLEANING_REQUIRED is only permitted from")
                    .contains("SEATED");
        }

        @Test
        @DisplayName("a rejection converts to a 409-bearing exception that keeps its context")
        void rejectionConvertsToException() {
            TableSnapshot uncleaned = Fixtures.uncleanedTable(Fixtures.T0, Fixtures.TURNAROUND_SLA);
            TransitionDecision.Rejected rejected = (TransitionDecision.Rejected) engine.decide(
                    uncleaned, Fixtures.signal(SignalType.GUEST_SEATED, Fixtures.T0), Fixtures.T0);

            InvalidTransitionException exception = rejected.toException();

            assertThat(exception.tableId()).isEqualTo(Fixtures.TABLE_ID);
            assertThat(exception.currentState()).isEqualTo(TableState.UNCLEANED);
            assertThat(exception.signalType()).isEqualTo(SignalType.GUEST_SEATED);
            assertThat(exception).hasMessage(rejected.narrative());
        }

        @ParameterizedTest(name = "a waiter acknowledgement against a {0} table is rejected")
        @EnumSource(value = TableState.class, names = {"FREE", "UNCLEANED"})
        @DisplayName("waiter acknowledgements are only meaningful for a seated table")
        void acknowledgementRejectedUnlessSeated(TableState state) {
            TableSnapshot table = Fixtures.tableIn(state, Fixtures.T0);

            TransitionDecision decision = engine.decide(
                    table, Fixtures.signal(SignalType.WAITER_ATTENDED, Fixtures.T0), Fixtures.T0);

            assertThat(decision).isInstanceOf(TransitionDecision.Rejected.class);
            assertThat(decision.narrative()).contains("only meaningful for a [SEATED] table");
        }
    }

    // ------------------------------------------------------------------ //
    // Acknowledgements
    // ------------------------------------------------------------------ //

    @Nested
    @DisplayName("waiter acknowledgement")
    class Acknowledgement {

        @Test
        @DisplayName("cancels the armed SLA without moving the table")
        void acknowledgementDisarmsTheSla() {
            TableSnapshot seated = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);
            Instant ackAt = Fixtures.T0.plus(Duration.ofMinutes(2));

            TransitionDecision.Applied applied = (TransitionDecision.Applied) engine.decide(
                    seated, Fixtures.signal(SignalType.WAITER_ATTENDED, ackAt), ackAt);

            assertThat(applied.next().state())
                    .as("the guests are still at the table")
                    .isEqualTo(TableState.SEATED);
            assertThat(applied.next().stateSince())
                    .as("acknowledging does not restart the dwell clock")
                    .isEqualTo(seated.stateSince());
            assertThat(applied.next().slaArmed()).isFalse();
            assertThat(applied.next().slaKind()).isNull();
            assertThat(applied.next().slaDeadline()).isNull();
            assertThat(applied.next().lastWaiterAckAt()).isEqualTo(ackAt);
            assertThat(applied.next().occupancy()).isEqualTo(seated.occupancy());
            assertThat(applied.next().version()).isEqualTo(seated.version() + 1L);
            assertThat(applied.timer().action()).isEqualTo(TimerAction.CANCEL);
            assertThat(applied.narrative()).contains("WAITER_RESPONSE SLA timer cancelled");
        }

        @Test
        @DisplayName("a second acknowledgement on an already-acknowledged table is ignored")
        void secondAcknowledgementIsIgnored() {
            Instant ackAt = Fixtures.T0.plus(Duration.ofMinutes(2));
            TableSnapshot acknowledged = Fixtures.acknowledgedTable(Fixtures.T0, ackAt);
            Instant secondAck = ackAt.plus(Duration.ofSeconds(5));

            TransitionDecision decision = engine.decide(
                    acknowledged, Fixtures.signal(SignalType.WAITER_ATTENDED, secondAck), secondAck);

            assertThat(decision).isInstanceOf(TransitionDecision.Ignored.class);
            assertThat(decision.narrative()).contains("already acknowledged");
        }

        @Test
        @DisplayName("records a first acknowledgement even when the SLA is switched off")
        void firstAcknowledgementIsRecordedWithoutAnSla() {
            TableStateEngine noSla = new TableStateEngine(Fixtures.policy(null, null));
            TableSnapshot seated = Fixtures.tableIn(TableState.SEATED, Fixtures.T0);
            Instant ackAt = Fixtures.T0.plus(Duration.ofMinutes(1));

            TransitionDecision decision =
                    noSla.decide(seated, Fixtures.signal(SignalType.WAITER_ATTENDED, ackAt), ackAt);

            assertThat(decision).isInstanceOf(TransitionDecision.Applied.class);
            TransitionDecision.Applied applied = (TransitionDecision.Applied) decision;
            assertThat(applied.next().lastWaiterAckAt()).isEqualTo(ackAt);
            assertThat(applied.narrative()).contains("no SLA timer cancelled");
        }
    }

    // ------------------------------------------------------------------ //
    // SLA policy interaction
    // ------------------------------------------------------------------ //

    @Nested
    @DisplayName("SLA policy")
    class SlaPolicyInteraction {

        @Test
        @DisplayName("a disabled waiter SLA cancels rather than leaves a stale timer armed")
        void disabledSlaCancels() {
            TableStateEngine noWaiterSla =
                    new TableStateEngine(Fixtures.policy(null, Fixtures.TURNAROUND_SLA));

            TransitionDecision.Applied applied = (TransitionDecision.Applied) noWaiterSla.decide(
                    Fixtures.freeTable(),
                    Fixtures.signal(SignalType.GUEST_SEATED, Fixtures.T0), Fixtures.T0);

            assertThat(applied.next().state()).isEqualTo(TableState.SEATED);
            assertThat(applied.next().slaArmed()).isFalse();
            assertThat(applied.timer().action())
                    .as("the table is leaving a state that may have had a timer; it must be cleared")
                    .isEqualTo(TimerAction.CANCEL);
        }

        @ParameterizedTest(name = "a {0} SLA window yields a CANCEL directive")
        @MethodSource("com.satyendra.restostream.engine.TableStateEngineTest#nonPositiveWindows")
        @DisplayName("a zero or negative SLA window is treated as disabled")
        void nonPositiveWindowCancels(String label, Duration window) {
            TableStateEngine engineWithWindow =
                    new TableStateEngine(Fixtures.policy(window, window));

            TimerDirective directive =
                    engineWithWindow.timerDirectiveFor(TableState.SEATED, Fixtures.T0);

            assertThat(label).isNotBlank();
            assertThat(directive.action()).isEqualTo(TimerAction.CANCEL);
        }

        @Test
        @DisplayName("a state that is not SLA tracked never consults the policy")
        void untrackedStateShortCircuitsThePolicy() {
            SlaPolicy policy = Mockito.mock(SlaPolicy.class);
            TableStateEngine engineWithMock = new TableStateEngine(policy);

            TimerDirective directive = engineWithMock.timerDirectiveFor(TableState.FREE, Fixtures.T0);

            assertThat(directive.action()).isEqualTo(TimerAction.CANCEL);
            Mockito.verifyNoInteractions(policy);
        }

        @Test
        @DisplayName("consults the policy exactly once per SLA-tracked transition")
        void consultsPolicyOncePerTrackedTransition() {
            SlaPolicy policy = Mockito.mock(SlaPolicy.class);
            Mockito.when(policy.timeoutFor(SlaKind.WAITER_RESPONSE))
                    .thenReturn(Optional.of(Fixtures.WAITER_SLA));
            TableStateEngine engineWithMock = new TableStateEngine(policy);

            engineWithMock.decide(Fixtures.freeTable(),
                    Fixtures.signal(SignalType.GUEST_SEATED, Fixtures.T0), Fixtures.T0);

            Mockito.verify(policy).timeoutFor(SlaKind.WAITER_RESPONSE);
            Mockito.verifyNoMoreInteractions(policy);
        }

        @Test
        @DisplayName("arms the turnaround SLA when entering UNCLEANED")
        void armsTurnaroundForUncleaned() {
            TimerDirective directive = engine.timerDirectiveFor(TableState.UNCLEANED, Fixtures.T0);

            assertThat(directive.action()).isEqualTo(TimerAction.ARM);
            assertThat(directive.slaKind()).isEqualTo(SlaKind.TABLE_TURNAROUND);
            assertThat(directive.ttl()).isEqualTo(Fixtures.TURNAROUND_SLA);
            assertThat(directive.deadline()).isEqualTo(Fixtures.T0.plus(Fixtures.TURNAROUND_SLA));
        }

        @Test
        @DisplayName("rejects null arguments when resolving a timer directive")
        void timerDirectiveRejectsNulls() {
            assertThatNullPointerException()
                    .isThrownBy(() -> engine.timerDirectiveFor(null, Fixtures.T0))
                    .withMessageContaining("target");
            assertThatNullPointerException()
                    .isThrownBy(() -> engine.timerDirectiveFor(TableState.SEATED, null))
                    .withMessageContaining("now");
        }
    }

    // ------------------------------------------------------------------ //
    // Breach evaluation — the timer races
    // ------------------------------------------------------------------ //

    @Nested
    @DisplayName("breach evaluation")
    class BreachEvaluation {

        @Test
        @DisplayName("confirms a genuine, un-alerted breach and raises the breach flag")
        void confirmsGenuineBreach() {
            TableSnapshot seated = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);
            Instant firedAt = seated.slaDeadline().plus(Duration.ofMillis(120));

            BreachDecision decision =
                    engine.evaluateBreach(seated, SlaKind.WAITER_RESPONSE, firedAt);

            assertThat(decision).isInstanceOf(BreachDecision.Confirmed.class);
            assertThat(decision.isConfirmed()).isTrue();
            BreachDecision.Confirmed confirmed = (BreachDecision.Confirmed) decision;

            assertThat(confirmed.slaKind()).isEqualTo(SlaKind.WAITER_RESPONSE);
            assertThat(confirmed.next().slaBreached()).isTrue();
            assertThat(confirmed.next().version()).isEqualTo(seated.version() + 1L);
            assertThat(confirmed.next().state())
                    .as("a breach is an annotation, not a transition")
                    .isEqualTo(TableState.SEATED);
            assertThat(confirmed.next().slaDeadline())
                    .as("the missed deadline is retained for forensic display")
                    .isEqualTo(seated.slaDeadline());
            assertThat(confirmed.narrative())
                    .contains("breached its WAITER_RESPONSE SLA")
                    .contains("overdue by");
        }

        @Test
        @DisplayName("confirms a breach detected exactly on the deadline")
        void confirmsBreachExactlyOnTheDeadline() {
            TableSnapshot seated = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);

            BreachDecision decision =
                    engine.evaluateBreach(seated, SlaKind.WAITER_RESPONSE, seated.slaDeadline());

            assertThat(decision.isConfirmed()).isTrue();
        }

        @Test
        @DisplayName("discards a firing for a table a waiter has since acknowledged")
        void discardsFiringAfterAcknowledgement() {
            Instant ackAt = Fixtures.T0.plus(Duration.ofMinutes(4));
            TableSnapshot acknowledged = Fixtures.acknowledgedTable(Fixtures.T0, ackAt);

            BreachDecision decision = engine.evaluateBreach(
                    acknowledged, SlaKind.WAITER_RESPONSE, Fixtures.T0.plus(Fixtures.WAITER_SLA));

            assertThat(decision).isInstanceOf(BreachDecision.Stale.class);
            assertThat(decision.isConfirmed()).isFalse();
            assertThat(decision.narrative()).contains("has no armed SLA");
        }

        @Test
        @DisplayName("discards a firing belonging to a superseded SLA class")
        void discardsFiringFromAPreviousState() {
            TableSnapshot uncleaned = Fixtures.uncleanedTable(Fixtures.T0, Fixtures.TURNAROUND_SLA);

            BreachDecision decision = engine.evaluateBreach(
                    uncleaned, SlaKind.WAITER_RESPONSE, uncleaned.slaDeadline());

            assertThat(decision).isInstanceOf(BreachDecision.Stale.class);
            assertThat(decision.narrative())
                    .contains("now guarded by TABLE_TURNAROUND")
                    .contains("belongs to a previous state");
        }

        @Test
        @DisplayName("suppresses a duplicate alert for an already-breached table")
        void suppressesDuplicateAlerts() {
            TableSnapshot seated = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);
            TableSnapshot alreadyBreached = seated.breached(seated.slaDeadline());

            BreachDecision decision = engine.evaluateBreach(alreadyBreached,
                    SlaKind.WAITER_RESPONSE, seated.slaDeadline().plus(Duration.ofSeconds(1)));

            assertThat(decision).isInstanceOf(BreachDecision.Stale.class);
            assertThat(decision.narrative()).contains("suppressing duplicate");
        }

        @Test
        @DisplayName("discards a firing that arrives before the deadline")
        void discardsEarlyFiring() {
            TableSnapshot seated = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);
            Instant tooEarly = seated.slaDeadline().minus(Duration.ofSeconds(30));

            BreachDecision decision =
                    engine.evaluateBreach(seated, SlaKind.WAITER_RESPONSE, tooEarly);

            assertThat(decision).isInstanceOf(BreachDecision.Stale.class);
            assertThat(decision.narrative()).contains("early");
        }

        @Test
        @DisplayName("rejects null arguments")
        void rejectsNulls() {
            TableSnapshot seated = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);
            assertThatNullPointerException()
                    .isThrownBy(() -> engine.evaluateBreach(null, SlaKind.WAITER_RESPONSE, Fixtures.T0))
                    .withMessageContaining("current");
            assertThatNullPointerException()
                    .isThrownBy(() -> engine.evaluateBreach(seated, null, Fixtures.T0))
                    .withMessageContaining("firedKind");
            assertThatNullPointerException()
                    .isThrownBy(() -> engine.evaluateBreach(seated, SlaKind.WAITER_RESPONSE, null))
                    .withMessageContaining("now");
        }
    }

    // ------------------------------------------------------------------ //
    // Narrative rendering
    // ------------------------------------------------------------------ //

    @Nested
    @DisplayName("timer narrative")
    class TimerNarrative {

        @Test
        @DisplayName("describes an ARM directive with its SLA, window and deadline")
        void describesArm() {
            String narrative = TableStateEngine.describeTimer(TimerDirective.arm(
                    SlaKind.WAITER_RESPONSE, Fixtures.WAITER_SLA,
                    Fixtures.T0.plus(Fixtures.WAITER_SLA)));

            assertThat(narrative)
                    .contains("WAITER_RESPONSE SLA armed")
                    .contains("PT5M");
        }

        @Test
        @DisplayName("describes a CANCEL directive")
        void describesCancel() {
            assertThat(TableStateEngine.describeTimer(TimerDirective.cancel()))
                    .isEqualTo("; any armed SLA timer cancelled");
        }

        @Test
        @DisplayName("describes a NONE directive as nothing at all")
        void describesNone() {
            assertThat(TableStateEngine.describeTimer(TimerDirective.none())).isEmpty();
        }
    }

    // ------------------------------------------------------------------ //
    // The full decision matrix
    // ------------------------------------------------------------------ //

    @ParameterizedTest(name = "[{0}] + {1} => {2}")
    @MethodSource("decisionMatrix")
    @DisplayName("the complete 3-state by 5-signal decision matrix is exhaustively specified")
    void decisionMatrixIsTotal(TableState state, SignalType signal, Class<?> expectedOutcome) {
        TableSnapshot table = switch (state) {
            case FREE -> Fixtures.freeTable();
            case SEATED -> Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);
            case UNCLEANED -> Fixtures.uncleanedTable(Fixtures.T0, Fixtures.TURNAROUND_SLA);
        };

        TransitionDecision decision =
                engine.decide(table, Fixtures.signal(signal, Fixtures.T0), Fixtures.T0);

        assertThat(decision).isInstanceOf(expectedOutcome);
        assertThat(decision.signal().type()).isEqualTo(signal);
        assertThat(decision.narrative()).isNotBlank();
    }

    /**
     * The authoritative decision matrix. Fifteen rows, one per state/signal pair, so that any
     * change to a guard shows up here as a named failure rather than as a silent behaviour change.
     *
     * @return one argument triple per cell of the matrix
     */
    static Stream<Arguments> decisionMatrix() {
        Class<?> applied = TransitionDecision.Applied.class;
        Class<?> ignored = TransitionDecision.Ignored.class;
        Class<?> rejected = TransitionDecision.Rejected.class;
        return Stream.of(
                // FREE
                Arguments.of(TableState.FREE, SignalType.GUEST_SEATED, applied),
                Arguments.of(TableState.FREE, SignalType.GUEST_DEPARTED, rejected),
                Arguments.of(TableState.FREE, SignalType.WAITER_ATTENDED, rejected),
                Arguments.of(TableState.FREE, SignalType.CLEANING_COMPLETED, ignored),
                Arguments.of(TableState.FREE, SignalType.CLEANING_REQUIRED, applied),
                // SEATED
                Arguments.of(TableState.SEATED, SignalType.GUEST_SEATED, ignored),
                Arguments.of(TableState.SEATED, SignalType.GUEST_DEPARTED, applied),
                Arguments.of(TableState.SEATED, SignalType.WAITER_ATTENDED, applied),
                Arguments.of(TableState.SEATED, SignalType.CLEANING_COMPLETED, rejected),
                Arguments.of(TableState.SEATED, SignalType.CLEANING_REQUIRED, rejected),
                // UNCLEANED
                Arguments.of(TableState.UNCLEANED, SignalType.GUEST_SEATED, rejected),
                Arguments.of(TableState.UNCLEANED, SignalType.GUEST_DEPARTED, ignored),
                Arguments.of(TableState.UNCLEANED, SignalType.WAITER_ATTENDED, rejected),
                Arguments.of(TableState.UNCLEANED, SignalType.CLEANING_COMPLETED, applied),
                Arguments.of(TableState.UNCLEANED, SignalType.CLEANING_REQUIRED, ignored));
    }

    /**
     * @return SLA windows that must all be treated as "switched off"
     */
    static Stream<Arguments> nonPositiveWindows() {
        return Stream.of(
                Arguments.of("zero", Duration.ZERO),
                Arguments.of("negative", Duration.ofSeconds(-30)),
                Arguments.of("disabled", (Duration) null));
    }
}
