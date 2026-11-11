/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.ingestion;

import com.satyendra.restostream.domain.OccupancySignal;
import com.satyendra.restostream.domain.SignalSourceKind;
import com.satyendra.restostream.domain.SignalType;
import com.satyendra.restostream.support.Fixtures;
import com.satyendra.restostream.support.MutableClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Unit suite for the CCTV motion conditioner — the component that makes the "real-time without
 * AI" requirement true.</p>
 *
 * <p>Every rule is asserted in isolation and then in combination, because the whole value of
 * choosing arithmetic over inference is that the behaviour is <em>completely specified</em>: given a
 * policy and a sequence of motion ratios, the emitted signals are a mathematical consequence, not a
 * probability. These tests are therefore the specification of the detector, and they run in
 * microseconds against a hand-driven clock.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@DisplayName("CctvMotionSignalSource")
class CctvMotionSignalSourceTest {

    private static final MotionConditioningPolicy POLICY = new MotionConditioningPolicy(
            0.18d, 0.04d, 3, 0.45d, Duration.ofSeconds(20), Duration.ofMinutes(30));

    private MutableClock clock;
    private CctvMotionSignalSource source;
    private List<OccupancySignal> emitted;
    private Disposable subscription;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(Fixtures.T0);
        source = new CctvMotionSignalSource("cctv-test", POLICY, clock, 64, true);
        emitted = new CopyOnWriteArrayList<>();
        subscription = source.signals().doOnNext(emitted::add).subscribe();
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        subscription.dispose();
    }

    private MotionAlert alert(String tableId, double ratio, double confidence) {
        return new MotionAlert(UUID.randomUUID().toString(), "cam-1", tableId, "WINDOW",
                clock.instant(), ratio, Math.round(ratio * 90_000), 2, confidence);
    }

    private MotionIngestOutcome post(String tableId, double ratio) {
        return post(tableId, ratio, 0.9d);
    }

    private MotionIngestOutcome post(String tableId, double ratio, double confidence) {
        return source.accept(alert(tableId, ratio, confidence)).block(Duration.ofSeconds(5));
    }

    // ------------------------------------------------------------------ //

    @Nested
    @DisplayName("confirmation streak")
    class ConfirmationStreak {

        @Test
        @DisplayName("requires three consecutive qualifying alerts before emitting")
        void requiresThreeConsecutiveAlerts() {
            assertThat(post("T-01", 0.40d).disposition())
                    .isEqualTo(MotionIngestOutcome.Disposition.AWAITING_CONFIRMATION);
            assertThat(post("T-01", 0.40d).disposition())
                    .isEqualTo(MotionIngestOutcome.Disposition.AWAITING_CONFIRMATION);
            assertThat(emitted).isEmpty();

            MotionIngestOutcome third = post("T-01", 0.40d);

            assertThat(third.disposition()).isEqualTo(MotionIngestOutcome.Disposition.EMITTED);
            assertThat(third.emitted()).contains(SignalType.GUEST_SEATED);
            assertThat(third.emittedSignal()).isEqualTo(SignalType.GUEST_SEATED);
            assertThat(third.occupiedStreak()).isEqualTo(3);
            assertThat(third.vacantStreak()).isZero();
            assertThat(third.narrative()).contains("confirmed by 3 consecutive alerts");
            assertThat(emitted).hasSize(1);
        }

        @Test
        @DisplayName("a waiter walking past the camera never produces a signal")
        void rejectsTransientMotion() {
            post("T-01", 0.55d);
            post("T-01", 0.01d);
            post("T-01", 0.55d);
            post("T-01", 0.01d);

            assertThat(emitted)
                    .as("alternating readings reset the streak, so nothing is ever confirmed")
                    .isEmpty();
        }

        @Test
        @DisplayName("an opposing reading resets the accumulated streak")
        void opposingReadingResetsStreak() {
            post("T-01", 0.40d);
            post("T-01", 0.40d);

            MotionIngestOutcome opposing = post("T-01", 0.01d);

            assertThat(opposing.occupiedStreak()).isZero();
            assertThat(opposing.vacantStreak()).isEqualTo(1);
        }

        @Test
        @DisplayName("caps the streak counter so a long-running camera cannot overflow it")
        void capsStreakCounter() {
            for (int i = 0; i < 50; i++) {
                post("T-01", 0.01d);
            }

            assertThat(post("T-01", 0.01d).vacantStreak())
                    .isEqualTo(POLICY.confirmationFrames());
        }
    }

    @Nested
    @DisplayName("hysteresis")
    class Hysteresis {

        @ParameterizedTest(name = "a motion ratio of {0} falls inside the dead band")
        @ValueSource(doubles = {0.05d, 0.10d, 0.17d, 0.1799d})
        @DisplayName("a reading inside the dead band changes nothing")
        void deadBandIsInert(double ratio) {
            MotionIngestOutcome outcome = post("T-01", ratio);

            assertThat(outcome.disposition())
                    .isEqualTo(MotionIngestOutcome.Disposition.AMBIGUOUS);
            assertThat(outcome.emitted()).isEmpty();
            assertThat(outcome.narrative()).contains("hysteresis dead band");
        }

        @Test
        @DisplayName("preserves accumulated evidence across a flicker into the dead band")
        void deadBandPreservesEvidence() {
            post("T-01", 0.40d);
            post("T-01", 0.40d);

            MotionIngestOutcome ambiguous = post("T-01", 0.12d);
            assertThat(ambiguous.occupiedStreak())
                    .as("a momentary flicker must not erase two frames of evidence")
                    .isEqualTo(2);

            assertThat(post("T-01", 0.40d).disposition())
                    .isEqualTo(MotionIngestOutcome.Disposition.EMITTED);
        }

        @Test
        @DisplayName("treats the thresholds themselves as inclusive")
        void thresholdsAreInclusive() {
            assertThat(post("T-01", POLICY.occupiedMotionRatio()).disposition())
                    .isNotEqualTo(MotionIngestOutcome.Disposition.AMBIGUOUS);
            assertThat(post("T-02", POLICY.vacantMotionRatio()).disposition())
                    .isNotEqualTo(MotionIngestOutcome.Disposition.AMBIGUOUS);
        }
    }

    @Nested
    @DisplayName("confidence floor")
    class ConfidenceFloor {

        @Test
        @DisplayName("discards an alert below the confidence floor without disturbing evidence")
        void lowConfidenceIsDiscarded() {
            post("T-01", 0.40d);
            post("T-01", 0.40d);

            MotionIngestOutcome blinded = post("T-01", 0.90d, 0.10d);

            assertThat(blinded.disposition())
                    .isEqualTo(MotionIngestOutcome.Disposition.LOW_CONFIDENCE);
            assertThat(blinded.occupiedStreak())
                    .as("a momentarily blinded camera must not erase accumulated evidence")
                    .isEqualTo(2);
            assertThat(blinded.narrative()).contains("below the 0.45 floor");

            assertThat(post("T-01", 0.40d).disposition())
                    .isEqualTo(MotionIngestOutcome.Disposition.EMITTED);
        }
    }

    @Nested
    @DisplayName("debounce and duplicate suppression")
    class DebounceAndDuplicates {

        @Test
        @DisplayName("assumes a table is vacant at genesis, so a quiet camera emits nothing")
        void quietCameraIsSilent() {
            for (int i = 0; i < 10; i++) {
                assertThat(post("T-01", 0.005d).disposition())
                        .isEqualTo(MotionIngestOutcome.Disposition.DUPLICATE_ASSERTION);
            }

            assertThat(emitted)
                    .as("a camera watching an empty table all morning must not raise a departure")
                    .isEmpty();
        }

        @Test
        @DisplayName("never re-asserts a judgement it has already emitted")
        void suppressesDuplicateJudgements() {
            post("T-01", 0.40d);
            post("T-01", 0.40d);
            post("T-01", 0.40d);

            MotionIngestOutcome again = post("T-01", 0.40d);

            assertThat(again.disposition())
                    .isEqualTo(MotionIngestOutcome.Disposition.DUPLICATE_ASSERTION);
            assertThat(again.narrative()).contains("not re-asserting");
            assertThat(emitted).hasSize(1);
        }

        @Test
        @DisplayName("holds a confirmed reversal until the debounce window closes")
        void debouncesRapidReversal() {
            post("T-01", 0.40d);
            post("T-01", 0.40d);
            post("T-01", 0.40d);
            assertThat(emitted).hasSize(1);

            clock.advance(Duration.ofSeconds(5));
            post("T-01", 0.01d);
            post("T-01", 0.01d);
            MotionIngestOutcome debounced = post("T-01", 0.01d);

            assertThat(debounced.disposition())
                    .isEqualTo(MotionIngestOutcome.Disposition.DEBOUNCED);
            assertThat(debounced.narrative()).contains("debounce window");
            assertThat(emitted).hasSize(1);

            clock.advance(Duration.ofSeconds(16));
            MotionIngestOutcome released = post("T-01", 0.01d);

            assertThat(released.disposition()).isEqualTo(MotionIngestOutcome.Disposition.EMITTED);
            assertThat(released.emitted()).contains(SignalType.GUEST_DEPARTED);
            assertThat(emitted).hasSize(2);
        }
    }

    @Nested
    @DisplayName("emitted signals")
    class EmittedSignals {

        @Test
        @DisplayName("stamps provenance, confidence and diagnostic attributes onto the signal")
        void signalCarriesProvenance() {
            post("T-01", 0.40d);
            post("T-01", 0.40d);
            post("T-01", 0.40d);

            OccupancySignal signal = emitted.get(0);

            assertThat(signal.tableId()).isEqualTo("T-01");
            assertThat(signal.type()).isEqualTo(SignalType.GUEST_SEATED);
            assertThat(signal.sourceKind()).isEqualTo(SignalSourceKind.CCTV_MOTION);
            assertThat(signal.sourceId()).isEqualTo("cam-1");
            assertThat(signal.confidence()).isEqualTo(0.9d);
            assertThat(signal.occupancy())
                    .as("the blob-count hint becomes the headcount for a seating signal")
                    .isEqualTo(2);
            assertThat(signal.attributes())
                    .containsEntry("cameraId", "cam-1")
                    .containsEntry("confirmationStreak", "3")
                    .containsEntry("decisionRule", "threshold+hysteresis+streak(no-ml)")
                    .containsEntry("zone", "WINDOW")
                    .containsKeys("motionRatio", "pixelDelta");
        }

        @Test
        @DisplayName("reports no headcount on a departure signal")
        void departureCarriesNoHeadcount() {
            post("T-01", 0.40d);
            post("T-01", 0.40d);
            post("T-01", 0.40d);
            clock.advance(Duration.ofSeconds(30));
            post("T-01", 0.01d);
            post("T-01", 0.01d);
            post("T-01", 0.01d);

            assertThat(emitted).hasSize(2);
            assertThat(emitted.get(1).type()).isEqualTo(SignalType.GUEST_DEPARTED);
            assertThat(emitted.get(1).occupancy()).isZero();
        }

        @Test
        @DisplayName("reuses the alert identifier as the signal's de-duplication key")
        void alertIdBecomesSignalId() {
            MotionAlert first = alert("T-01", 0.40d, 0.9d);
            source.accept(first).block(Duration.ofSeconds(5));
            source.accept(alert("T-01", 0.40d, 0.9d)).block(Duration.ofSeconds(5));
            MotionAlert third = alert("T-01", 0.40d, 0.9d);
            source.accept(third).block(Duration.ofSeconds(5));

            assertThat(emitted).hasSize(1);
            assertThat(emitted.get(0).signalId()).isEqualTo(third.alertId());
            assertThat(first.alertId()).isNotEqualTo(third.alertId());
        }
    }

    @Nested
    @DisplayName("per-table isolation and housekeeping")
    class IsolationAndHousekeeping {

        @Test
        @DisplayName("tracks each table's evidence independently")
        void tablesAreIndependent() {
            post("T-01", 0.40d);
            post("T-01", 0.40d);
            post("T-02", 0.40d);

            assertThat(emitted).isEmpty();
            assertThat(source.trackedTables()).isEqualTo(2);

            assertThat(post("T-01", 0.40d).disposition())
                    .isEqualTo(MotionIngestOutcome.Disposition.EMITTED);
            assertThat(post("T-02", 0.40d).disposition())
                    .isEqualTo(MotionIngestOutcome.Disposition.AWAITING_CONFIRMATION);
        }

        @Test
        @DisplayName("evicts evidence for a camera that has gone silent")
        void evictsIdleTracks() {
            post("T-01", 0.40d);
            post("T-02", 0.40d);
            assertThat(source.trackedTables()).isEqualTo(2);

            assertThat(source.evictIdleTracks(clock.instant())).isZero();

            clock.advance(POLICY.trackRetention().plusMinutes(1));
            post("T-02", 0.40d);

            assertThat(source.housekeep(clock.instant()))
                    .as("T-01 has gone silent for longer than the retention window; T-02 has not")
                    .isEqualTo(1);
            assertThat(source.trackedTables()).isEqualTo(1);
            assertThatNullPointerException().isThrownBy(() -> source.evictIdleTracks(null));
        }

        @Test
        @DisplayName("counts alerts conditioned and signals emitted")
        void maintainsCounters() {
            post("T-01", 0.40d);
            post("T-01", 0.12d);
            post("T-01", 0.40d);
            post("T-01", 0.40d);

            assertThat(source.alertsAccepted()).isEqualTo(4);
            assertThat(source.signalsEmitted()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("transport contract")
    class TransportContract {

        @Test
        @DisplayName("declares its identity, kind and enabled state")
        void declaresIdentity() {
            assertThat(source.sourceId()).isEqualTo("cctv-test");
            assertThat(source.kind()).isEqualTo(SignalSourceKind.CCTV_MOTION);
            assertThat(source.enabled()).isTrue();
            assertThat(new CctvMotionSignalSource("off", POLICY, clock, 8, false).enabled())
                    .isFalse();
        }

        @Test
        @DisplayName("defends its construction and call contracts")
        void defendsContracts() {
            assertThatNullPointerException()
                    .isThrownBy(() -> new CctvMotionSignalSource(null, POLICY, clock, 8, true));
            assertThatNullPointerException()
                    .isThrownBy(() -> new CctvMotionSignalSource("x", null, clock, 8, true));
            assertThatNullPointerException()
                    .isThrownBy(() -> new CctvMotionSignalSource("x", POLICY, null, 8, true));
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> new CctvMotionSignalSource("x", POLICY, clock, 0, true));
            assertThatNullPointerException().isThrownBy(() -> source.accept(null));
        }

        @Test
        @DisplayName("exposes a non-null signal stream before anything is published")
        void signalStreamIsAlwaysAvailable() {
            Flux<OccupancySignal> stream =
                    new CctvMotionSignalSource("fresh", POLICY, clock, 8, true).signals();

            assertThat(stream).isNotNull();
        }
    }
}
