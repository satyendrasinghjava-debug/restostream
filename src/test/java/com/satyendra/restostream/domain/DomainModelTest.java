/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.domain;

import com.satyendra.restostream.support.Fixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Unit suite for the immutable domain model. These types carry the system's invariants, so the
 * assertions here are about what the model <em>refuses</em> as much as what it accepts: an
 * out-of-range confidence, a negative occupancy or a blank table identifier must fail at the point
 * of construction rather than ten layers downstream in a Redis write.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@DisplayName("domain model")
class DomainModelTest {

    @Nested
    @DisplayName("TableState")
    class TableStateTest {

        @Test
        @DisplayName("maps each state to its mandated dashboard colour")
        void coloursMatchTheSpecification() {
            assertThat(TableState.FREE.colourName()).isEqualTo("GREEN");
            assertThat(TableState.FREE.colourHex()).isEqualTo("#2E7D32");
            assertThat(TableState.SEATED.colourName()).isEqualTo("RED");
            assertThat(TableState.SEATED.colourHex()).isEqualTo("#C62828");
            assertThat(TableState.UNCLEANED.colourName()).isEqualTo("AMBER");
            assertThat(TableState.UNCLEANED.colourHex()).isEqualTo("#F9A825");
        }

        @Test
        @DisplayName("encodes the lifecycle graph")
        void transitionGraphIsClosed() {
            assertThat(TableState.FREE.allowedSuccessors())
                    .containsExactlyInAnyOrder(TableState.SEATED, TableState.UNCLEANED);
            assertThat(TableState.SEATED.allowedSuccessors())
                    .containsExactly(TableState.UNCLEANED);
            assertThat(TableState.UNCLEANED.allowedSuccessors())
                    .containsExactly(TableState.FREE);
        }

        @Test
        @DisplayName("guards transitions and tolerates a null target")
        void guardsTransitions() {
            assertThat(TableState.FREE.canTransitionTo(TableState.SEATED)).isTrue();
            assertThat(TableState.FREE.canTransitionTo(TableState.FREE)).isFalse();
            assertThat(TableState.SEATED.canTransitionTo(TableState.FREE)).isFalse();
            assertThat(TableState.UNCLEANED.canTransitionTo(TableState.SEATED)).isFalse();
            assertThat(TableState.FREE.canTransitionTo(null)).isFalse();
        }

        @Test
        @DisplayName("declares which states carry an SLA and which SLA that is")
        void slaTracking() {
            assertThat(TableState.FREE.slaTracked()).isFalse();
            assertThat(TableState.FREE.slaKind()).isNull();
            assertThat(TableState.SEATED.slaTracked()).isTrue();
            assertThat(TableState.SEATED.slaKind()).isEqualTo(SlaKind.WAITER_RESPONSE);
            assertThat(TableState.UNCLEANED.slaTracked()).isTrue();
            assertThat(TableState.UNCLEANED.slaKind()).isEqualTo(SlaKind.TABLE_TURNAROUND);
        }

        @ParameterizedTest
        @EnumSource(TableState.class)
        @DisplayName("every state exposes a complete, immutable description")
        void everyStateIsFullyDescribed(TableState state) {
            assertThat(state.colourName()).isNotBlank();
            assertThat(state.colourHex()).startsWith("#").hasSize(7);
            assertThat(state.allowedSuccessors()).doesNotContain(state);
            assertThatExceptionOfType(UnsupportedOperationException.class)
                    .isThrownBy(() -> state.allowedSuccessors().add(TableState.FREE));
        }
    }

    @Nested
    @DisplayName("SignalType")
    class SignalTypeTest {

        @Test
        @DisplayName("declares the state each signal drives a table towards")
        void targetStates() {
            assertThat(SignalType.GUEST_SEATED.targetState()).contains(TableState.SEATED);
            assertThat(SignalType.GUEST_DEPARTED.targetState()).contains(TableState.UNCLEANED);
            assertThat(SignalType.CLEANING_COMPLETED.targetState()).contains(TableState.FREE);
            assertThat(SignalType.CLEANING_REQUIRED.targetState()).contains(TableState.UNCLEANED);
            assertThat(SignalType.WAITER_ATTENDED.targetState()).isEmpty();
        }

        @Test
        @DisplayName("marks the waiter acknowledgement as the only transition-free signal")
        void acknowledgementIsTheOnlyNonTransition() {
            assertThat(SignalType.WAITER_ATTENDED.isAcknowledgement()).isTrue();
            for (SignalType type : SignalType.values()) {
                if (type != SignalType.WAITER_ATTENDED) {
                    assertThat(type.isAcknowledgement()).isFalse();
                }
            }
        }

        @Test
        @DisplayName("restricts housekeeping to a clean table and departure to a seated one")
        void operationalPolicyIsNarrowerThanTheGraph() {
            assertThat(SignalType.CLEANING_REQUIRED.permittedOriginStates())
                    .containsExactly(TableState.FREE);
            assertThat(SignalType.GUEST_DEPARTED.permittedOriginStates())
                    .containsExactly(TableState.SEATED);
            assertThat(SignalType.CLEANING_REQUIRED.isPermittedFrom(TableState.SEATED))
                    .as("both signals target UNCLEANED, but only departure may drive a seated table")
                    .isFalse();
            assertThat(SignalType.GUEST_DEPARTED.isPermittedFrom(TableState.SEATED)).isTrue();
            assertThat(SignalType.GUEST_DEPARTED.isPermittedFrom(null)).isFalse();
        }

        @ParameterizedTest
        @EnumSource(SignalType.class)
        @DisplayName("every signal declares a non-empty, immutable origin set")
        void everySignalIsFullyDescribed(SignalType type) {
            assertThat(type.permittedOriginStates()).isNotEmpty();
            assertThatExceptionOfType(UnsupportedOperationException.class)
                    .isThrownBy(() -> type.permittedOriginStates().clear());
        }
    }

    @Nested
    @DisplayName("SlaKind and SignalSourceKind")
    class SupportingEnums {

        @ParameterizedTest
        @EnumSource(SlaKind.class)
        @DisplayName("every SLA class carries an operator-facing description")
        void slaKindsAreDescribed(SlaKind kind) {
            assertThat(kind.description()).isNotBlank();
        }

        @ParameterizedTest
        @EnumSource(SignalSourceKind.class)
        @DisplayName("every transport carries a trust weight within 0..100")
        void trustWeightsAreBounded(SignalSourceKind kind) {
            assertThat(kind.trustWeight()).isBetween(0, 100);
        }

        @Test
        @DisplayName("ranks a human tap above a camera and a camera above a simulator")
        void trustOrdering() {
            assertThat(SignalSourceKind.MANUAL_TAP.trustWeight())
                    .isGreaterThan(SignalSourceKind.MOBILE_CAMERA.trustWeight());
            assertThat(SignalSourceKind.MOBILE_CAMERA.trustWeight())
                    .isGreaterThan(SignalSourceKind.CCTV_MOTION.trustWeight());
            assertThat(SignalSourceKind.CCTV_MOTION.trustWeight())
                    .isGreaterThan(SignalSourceKind.SIMULATOR.trustWeight());
        }
    }

    @Nested
    @DisplayName("OccupancySignal")
    class OccupancySignalTest {

        @Test
        @DisplayName("defaults a missing source identifier and an absent attribute map")
        void appliesDefaults() {
            OccupancySignal signal = new OccupancySignal("s1", "T-01", SignalType.GUEST_SEATED,
                    SignalSourceKind.CCTV_MOTION, null, Fixtures.T0, 0.8d, 2, null);

            assertThat(signal.sourceId()).isEqualTo("cctv_motion");
            assertThat(signal.attributes()).isEmpty();
        }

        @Test
        @DisplayName("defends its invariants at construction")
        void rejectsInvalidValues() {
            assertThatNullPointerException().isThrownBy(() -> new OccupancySignal(
                    null, "T-01", SignalType.GUEST_SEATED, SignalSourceKind.MANUAL_TAP,
                    "a", Fixtures.T0, 1.0d, 0, Map.of()));
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> new OccupancySignal("s", "  ", SignalType.GUEST_SEATED,
                            SignalSourceKind.MANUAL_TAP, "a", Fixtures.T0, 1.0d, 0, Map.of()))
                    .withMessageContaining("tableId must not be blank");
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> new OccupancySignal("s", "T-01", SignalType.GUEST_SEATED,
                            SignalSourceKind.MANUAL_TAP, "a", Fixtures.T0, 1.4d, 0, Map.of()))
                    .withMessageContaining("confidence");
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> new OccupancySignal("s", "T-01", SignalType.GUEST_SEATED,
                            SignalSourceKind.MANUAL_TAP, "a", Fixtures.T0, 1.0d, -1, Map.of()))
                    .withMessageContaining("occupancy");
        }

        @Test
        @DisplayName("copies rather than mutates when deriving a new signal")
        void derivationIsNonDestructive() {
            OccupancySignal original = OccupancySignal.trusted(
                    "T-01", SignalType.GUEST_SEATED, SignalSourceKind.MANUAL_TAP, "waiter-7", Fixtures.T0);

            OccupancySignal derived = original.withOccupancy(4).withAttribute("camera", "patio-3");

            assertThat(original.occupancy()).isZero();
            assertThat(original.attributes()).isEmpty();
            assertThat(derived.occupancy()).isEqualTo(4);
            assertThat(derived.attributes()).containsEntry("camera", "patio-3");
            assertThat(derived.signalId()).isEqualTo(original.signalId());
            assertThat(derived.confidence()).isEqualTo(1.0d);
            assertThatNullPointerException().isThrownBy(() -> original.withAttribute("k", null));
            assertThatNullPointerException().isThrownBy(() -> original.withAttribute(null, "v"));
        }

        @Test
        @DisplayName("keeps its attribute map immutable")
        void attributesAreImmutable() {
            OccupancySignal signal = Fixtures.signal(SignalType.GUEST_SEATED, Fixtures.T0);
            assertThatExceptionOfType(UnsupportedOperationException.class)
                    .isThrownBy(() -> signal.attributes().put("x", "y"));
        }
    }

    @Nested
    @DisplayName("TableSnapshot")
    class TableSnapshotTest {

        @Test
        @DisplayName("provisions a version-zero FREE table with no SLA")
        void provisionsGenesis() {
            TableSnapshot table = TableSnapshot.provision("T-07", "PATIO", 6, Fixtures.T0);

            assertThat(table.version()).isZero();
            assertThat(table.state()).isEqualTo(TableState.FREE);
            assertThat(table.slaArmed()).isFalse();
            assertThat(table.occupancy()).isZero();
            assertThat(table.slaBreached()).isFalse();
            assertThat(table.lastSignalType()).isNull();
            assertThat(table.lastWaiterAckAt()).isNull();
            assertThat(table.colourHex()).isEqualTo("#2E7D32");
        }

        @Test
        @DisplayName("defaults a missing zone and defends its invariants")
        void defaultsAndInvariants() {
            assertThat(TableSnapshot.provision("T-1", null, 2, Fixtures.T0).zone())
                    .isEqualTo("DEFAULT");
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> TableSnapshot.provision(" ", "Z", 2, Fixtures.T0))
                    .withMessageContaining("blank");
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> TableSnapshot.provision("T-1", "Z", -2, Fixtures.T0))
                    .withMessageContaining("seats");
            assertThatNullPointerException()
                    .isThrownBy(() -> TableSnapshot.provision("T-1", "Z", 2, null));
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> new TableSnapshot("T-1", "Z", 2, TableState.FREE,
                            Fixtures.T0, -1L, null, null, false, 0, null, null, null, Fixtures.T0))
                    .withMessageContaining("version");
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> new TableSnapshot("T-1", "Z", 2, TableState.FREE,
                            Fixtures.T0, 0L, null, null, false, -3, null, null, null, Fixtures.T0))
                    .withMessageContaining("occupancy");
        }

        @Test
        @DisplayName("computes remaining SLA time, flooring at zero and tolerating no SLA")
        void slaRemaining() {
            TableSnapshot seated = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);

            assertThat(seated.slaRemaining(Fixtures.T0)).isEqualTo(Fixtures.WAITER_SLA);
            assertThat(seated.slaRemaining(Fixtures.T0.plus(Duration.ofMinutes(2))))
                    .isEqualTo(Duration.ofMinutes(3));
            assertThat(seated.slaRemaining(Fixtures.T0.plus(Duration.ofHours(1))))
                    .isEqualTo(Duration.ZERO);
            assertThat(Fixtures.freeTable().slaRemaining(Fixtures.T0)).isEqualTo(Duration.ZERO);
            assertThatNullPointerException().isThrownBy(() -> seated.slaRemaining(null));
        }

        @Test
        @DisplayName("computes dwell time, flooring at zero for a clock that went backwards")
        void dwellTime() {
            TableSnapshot seated = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);

            assertThat(seated.dwellTime(Fixtures.T0.plus(Duration.ofMinutes(12))))
                    .isEqualTo(Duration.ofMinutes(12));
            assertThat(seated.dwellTime(Fixtures.T0.minus(Duration.ofMinutes(5))))
                    .isEqualTo(Duration.ZERO);
            assertThatNullPointerException().isThrownBy(() -> seated.dwellTime(null));
        }

        @Test
        @DisplayName("rewrites SLA fields wholesale on a transition so no stale deadline leaks")
        void transitionRewritesSlaFields() {
            TableSnapshot seated = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);
            Instant at = Fixtures.T0.plus(Duration.ofMinutes(30));

            TableSnapshot next = seated.transitionedTo(TableState.UNCLEANED,
                    Fixtures.signal(SignalType.GUEST_DEPARTED, at), null, null, at);

            assertThat(next.slaKind()).isNull();
            assertThat(next.slaDeadline()).isNull();
            assertThat(next.slaArmed()).isFalse();
            assertThat(next.slaBreached()).isFalse();
            assertThat(next.occupancy()).isZero();
            assertThat(next.lastWaiterAckAt()).isNull();
            assertThatNullPointerException().isThrownBy(() -> seated.transitionedTo(
                    null, Fixtures.signal(SignalType.GUEST_DEPARTED, at), null, null, at));
            assertThatNullPointerException().isThrownBy(() ->
                    seated.transitionedTo(TableState.UNCLEANED, null, null, null, at));
        }

        @Test
        @DisplayName("preserves state and occupancy through an acknowledgement")
        void acknowledgementPreservesState() {
            TableSnapshot seated = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);
            Instant at = Fixtures.T0.plus(Duration.ofMinutes(1));

            TableSnapshot next =
                    seated.acknowledged(Fixtures.signal(SignalType.WAITER_ATTENDED, at), at);

            assertThat(next.state()).isEqualTo(TableState.SEATED);
            assertThat(next.occupancy()).isEqualTo(seated.occupancy());
            assertThat(next.stateSince()).isEqualTo(seated.stateSince());
            assertThat(next.lastWaiterAckAt()).isEqualTo(at);
            assertThat(next.slaArmed()).isFalse();
            assertThatNullPointerException().isThrownBy(() -> seated.acknowledged(null, at));
        }

        @Test
        @DisplayName("retains the missed deadline when flagging a breach")
        void breachRetainsTheDeadline() {
            TableSnapshot seated = Fixtures.seatedTable(Fixtures.T0, Fixtures.WAITER_SLA);
            Instant at = seated.slaDeadline().plus(Duration.ofSeconds(2));

            TableSnapshot breached = seated.breached(at);

            assertThat(breached.slaBreached()).isTrue();
            assertThat(breached.slaDeadline()).isEqualTo(seated.slaDeadline());
            assertThat(breached.slaKind()).isEqualTo(seated.slaKind());
            assertThat(breached.state()).isEqualTo(seated.state());
            assertThat(breached.version()).isEqualTo(seated.version() + 1L);
            assertThatNullPointerException().isThrownBy(() -> seated.breached(null));
        }
    }
}
