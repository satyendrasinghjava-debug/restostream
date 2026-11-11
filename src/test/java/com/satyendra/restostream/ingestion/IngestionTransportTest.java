/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.ingestion;

import com.satyendra.restostream.domain.OccupancySignal;
import com.satyendra.restostream.domain.SignalSourceKind;
import com.satyendra.restostream.domain.SignalType;
import com.satyendra.restostream.support.Fixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
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
 * <p>Unit suite for the ingestion transport layer: the generic push source, the motion alert wire
 * format and the conditioning policy's own invariants.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@DisplayName("ingestion transports")
class IngestionTransportTest {

    @Nested
    @DisplayName("PushOccupancySignalSource")
    class PushSource {

        @Test
        @DisplayName("declares its identity and participates in ingestion by default")
        void declaresIdentity() {
            PushOccupancySignalSource source =
                    new PushOccupancySignalSource("manual-tap", SignalSourceKind.MANUAL_TAP, 16);

            assertThat(source.sourceId()).isEqualTo("manual-tap");
            assertThat(source.kind()).isEqualTo(SignalSourceKind.MANUAL_TAP);
            assertThat(source.enabled()).isTrue();
            assertThat(source.housekeep(Fixtures.T0))
                    .as("a stateless transport has nothing to reclaim")
                    .isZero();
        }

        @Test
        @DisplayName("publishes signals to its subscriber in order")
        void publishesInOrder() {
            PushOccupancySignalSource source =
                    new PushOccupancySignalSource("push", SignalSourceKind.MOBILE_CAMERA, 16);
            List<OccupancySignal> received = new CopyOnWriteArrayList<>();
            var subscription = source.signals().doOnNext(received::add).subscribe();

            OccupancySignal first = Fixtures.signal(SignalType.GUEST_SEATED, Fixtures.T0);
            OccupancySignal second = Fixtures.signal(
                    SignalType.WAITER_ATTENDED, Fixtures.T0.plusSeconds(30));

            StepVerifier.create(source.publish(first).then(source.publish(second)))
                    .verifyComplete();

            assertThat(received).containsExactly(first, second);
            subscription.dispose();
        }

        @Test
        @DisplayName("buffers signals published before any subscriber attaches")
        void buffersBeforeSubscription() {
            PushOccupancySignalSource source =
                    new PushOccupancySignalSource("push", SignalSourceKind.SIMULATOR, 16);
            OccupancySignal signal = Fixtures.signal(SignalType.GUEST_SEATED, Fixtures.T0);

            StepVerifier.create(source.publish(signal)).verifyComplete();

            StepVerifier.create(source.signals().take(1))
                    .expectNext(signal)
                    .verifyComplete();
        }

        @Test
        @DisplayName("refuses signals once its bounded buffer is saturated, surfacing the sink result")
        void refusesWhenSaturated() {
            PushOccupancySignalSource source =
                    new PushOccupancySignalSource("tiny", SignalSourceKind.SIMULATOR, 1);

            // No subscriber is attached, so the bounded warm-up buffer fills and then refuses.
            // Refusing loudly is the point: a silently dropped observation is a table that never
            // turns red.
            Throwable refusal = null;
            for (int i = 0; i < 64 && refusal == null; i++) {
                refusal = source.publish(Fixtures.signal(
                                SignalType.GUEST_SEATED, Fixtures.T0.plusSeconds(i)))
                        .then(reactor.core.publisher.Mono.<Throwable>empty())
                        .onErrorResume(reactor.core.publisher.Mono::just)
                        .block(Duration.ofSeconds(5));
            }

            assertThat(refusal)
                    .as("a saturated ingestion buffer must refuse rather than drop")
                    .isInstanceOf(PushOccupancySignalSource.SignalRejectedException.class)
                    .hasMessageContaining("refused the signal");
            assertThat(((PushOccupancySignalSource.SignalRejectedException) refusal).emitResult())
                    .isNotNull();
        }

        @Test
        @DisplayName("defends its construction and call contracts")
        void defendsContracts() {
            assertThatNullPointerException().isThrownBy(() ->
                    new PushOccupancySignalSource(null, SignalSourceKind.MANUAL_TAP, 8));
            assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() ->
                            new PushOccupancySignalSource("  ", SignalSourceKind.MANUAL_TAP, 8))
                    .withMessageContaining("must not be blank");
            assertThatNullPointerException().isThrownBy(() ->
                    new PushOccupancySignalSource("push", null, 8));
            assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() ->
                            new PushOccupancySignalSource("push", SignalSourceKind.MANUAL_TAP, 0))
                    .withMessageContaining("must be positive");
            assertThatNullPointerException().isThrownBy(() ->
                    new PushOccupancySignalSource("push", SignalSourceKind.MANUAL_TAP, 8)
                            .publish(null));
        }
    }

    @Nested
    @DisplayName("MotionAlert")
    class MotionAlertContract {

        private MotionAlert alert(double ratio, long pixelDelta, int hint, double confidence) {
            return new MotionAlert("a1", "cam", "T-01", "WINDOW",
                    Fixtures.T0, ratio, pixelDelta, hint, confidence);
        }

        @Test
        @DisplayName("accepts a well-formed alert and defaults a missing camera identifier")
        void acceptsWellFormedAlert() {
            MotionAlert alert = alert(0.25d, 22_500L, 3, 0.8d);

            assertThat(alert.tableId()).isEqualTo("T-01");
            assertThat(alert.motionRatio()).isEqualTo(0.25d);
            assertThat(alert.capturedAt()).isEqualTo(Fixtures.T0);

            MotionAlert anonymous = new MotionAlert("a2", "  ", "T-01", null,
                    Fixtures.T0, 0.1d, 0L, 0, 1.0d);
            assertThat(anonymous.cameraId()).isEqualTo("unknown-camera");
            assertThat(anonymous.zone()).isNull();
        }

        @Test
        @DisplayName("defends every physical invariant at construction")
        void defendsInvariants() {
            assertThatNullPointerException().isThrownBy(() -> new MotionAlert(
                    null, "cam", "T-01", null, Fixtures.T0, 0.1d, 0L, 0, 1.0d));
            assertThatNullPointerException().isThrownBy(() -> new MotionAlert(
                    "a", "cam", null, null, Fixtures.T0, 0.1d, 0L, 0, 1.0d));
            assertThatNullPointerException().isThrownBy(() -> new MotionAlert(
                    "a", "cam", "T-01", null, null, 0.1d, 0L, 0, 1.0d));
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> new MotionAlert("a", "cam", " ", null,
                            Fixtures.T0, 0.1d, 0L, 0, 1.0d))
                    .withMessageContaining("tableId must not be blank");
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> alert(1.5d, 0L, 0, 1.0d))
                    .withMessageContaining("motionRatio");
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> alert(-0.1d, 0L, 0, 1.0d))
                    .withMessageContaining("motionRatio");
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> alert(0.5d, 0L, 0, 1.5d))
                    .withMessageContaining("confidence");
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> alert(0.5d, -1L, 0, 1.0d))
                    .withMessageContaining("pixelDelta");
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> alert(0.5d, 0L, -1, 1.0d))
                    .withMessageContaining("occupancyHint");
        }
    }

    @Nested
    @DisplayName("MotionConditioningPolicy")
    class PolicyContract {

        @Test
        @DisplayName("ships production defaults with a genuine dead band")
        void defaultsAreUsable() {
            MotionConditioningPolicy defaults = MotionConditioningPolicy.defaults();

            assertThat(defaults.vacantMotionRatio()).isLessThan(defaults.occupiedMotionRatio());
            assertThat(defaults.confirmationFrames()).isGreaterThanOrEqualTo(1);
            assertThat(defaults.emitDebounce()).isEqualTo(Duration.ofSeconds(20));
            assertThat(defaults.trackRetention()).isEqualTo(Duration.ofMinutes(30));
            assertThat(defaults.minConfidence()).isBetween(0.0d, 1.0d);
        }

        @Test
        @DisplayName("refuses a policy whose thresholds leave no dead band")
        void refusesInvertedThresholds() {
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> new MotionConditioningPolicy(
                            0.04d, 0.18d, 3, 0.5d, Duration.ofSeconds(1), Duration.ofMinutes(1)))
                    .withMessageContaining("hysteresis dead band");
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> new MotionConditioningPolicy(
                            0.10d, 0.10d, 3, 0.5d, Duration.ofSeconds(1), Duration.ofMinutes(1)))
                    .withMessageContaining("hysteresis dead band");
        }

        @Test
        @DisplayName("refuses out-of-range or nonsensical tuning")
        void refusesInvalidTuning() {
            Duration second = Duration.ofSeconds(1);
            Duration minute = Duration.ofMinutes(1);

            assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() ->
                            new MotionConditioningPolicy(1.5d, 0.04d, 3, 0.5d, second, minute))
                    .withMessageContaining("occupiedMotionRatio");
            assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() ->
                            new MotionConditioningPolicy(0.18d, -0.1d, 3, 0.5d, second, minute))
                    .withMessageContaining("vacantMotionRatio");
            assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() ->
                            new MotionConditioningPolicy(0.18d, 0.04d, 0, 0.5d, second, minute))
                    .withMessageContaining("confirmationFrames");
            assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() ->
                            new MotionConditioningPolicy(0.18d, 0.04d, 3, 1.5d, second, minute))
                    .withMessageContaining("minConfidence");
            assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() ->
                            new MotionConditioningPolicy(0.18d, 0.04d, 3, 0.5d,
                                    Duration.ofSeconds(-1), minute))
                    .withMessageContaining("emitDebounce");
            assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() ->
                            new MotionConditioningPolicy(0.18d, 0.04d, 3, 0.5d, second, Duration.ZERO))
                    .withMessageContaining("trackRetention");
            assertThatNullPointerException().isThrownBy(() ->
                    new MotionConditioningPolicy(0.18d, 0.04d, 3, 0.5d, null, minute));
            assertThatNullPointerException().isThrownBy(() ->
                    new MotionConditioningPolicy(0.18d, 0.04d, 3, 0.5d, second, null));
        }

        @Test
        @DisplayName("permits a zero debounce, which means every confirmation is emitted")
        void permitsZeroDebounce() {
            MotionConditioningPolicy policy = new MotionConditioningPolicy(
                    0.18d, 0.04d, 1, 0.0d, Duration.ZERO, Duration.ofMinutes(1));

            assertThat(policy.emitDebounce()).isZero();
            assertThat(policy.confirmationFrames()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("MotionIngestOutcome")
    class OutcomeContract {

        @Test
        @DisplayName("reports an emitted signal as present and a non-emission as absent")
        void reportsEmission() {
            MotionIngestOutcome emitted = new MotionIngestOutcome("a1", "T-01",
                    MotionIngestOutcome.Disposition.EMITTED, SignalType.GUEST_SEATED, 3, 0, "ok");
            MotionIngestOutcome ambiguous = new MotionIngestOutcome("a2", "T-01",
                    MotionIngestOutcome.Disposition.AMBIGUOUS, null, 1, 0, "dead band");

            assertThat(emitted.emitted()).contains(SignalType.GUEST_SEATED);
            assertThat(ambiguous.emitted()).isEmpty();
        }

        @Test
        @DisplayName("defends its mandatory components")
        void defendsContracts() {
            Instant unused = Fixtures.T0;
            assertThat(unused).isNotNull();
            assertThatNullPointerException().isThrownBy(() -> new MotionIngestOutcome(
                    null, "T-01", MotionIngestOutcome.Disposition.EMITTED, null, 0, 0, "n"));
            assertThatNullPointerException().isThrownBy(() -> new MotionIngestOutcome(
                    "a", null, MotionIngestOutcome.Disposition.EMITTED, null, 0, 0, "n"));
            assertThatNullPointerException().isThrownBy(() -> new MotionIngestOutcome(
                    "a", "T-01", null, null, 0, 0, "n"));
            assertThatNullPointerException().isThrownBy(() -> new MotionIngestOutcome(
                    "a", "T-01", MotionIngestOutcome.Disposition.EMITTED, null, 0, 0, null));
        }
    }
}
