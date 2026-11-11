/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.ingestion;

import com.satyendra.restostream.domain.OccupancySignal;
import com.satyendra.restostream.domain.SignalSourceKind;
import com.satyendra.restostream.domain.SignalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>The CCTV ingestion adapter: converts a high-rate stream of cheap pixel/motion alerts into a
 * low-rate stream of trustworthy occupancy signals, using only thresholding, hysteresis, streak
 * confirmation and debouncing.</p>
 *
 * <p><strong>No AI, no machine learning, no model — by design.</strong> The entire judgement is the
 * arithmetic described in {@link MotionConditioningPolicy}, executed in constant time and constant
 * memory per table. That is what lets RestoStream hold a real-time guarantee: the hot path has no
 * inference latency to amortise, no GPU to contend for, no model version to roll out, and its
 * worst-case cost is a handful of floating-point comparisons.</p>
 *
 * <h2>Today and tomorrow, through the same code path</h2>
 * <p>Today a mobile camera or a dashboard tap posts {@link MotionAlert}s to
 * {@code POST /api/v1/ingest/cctv/motion}. Tomorrow an NVR or an OpenCV background-subtractor
 * process posts the identical payload straight from the video pipeline. Nothing in this class, the
 * state engine, the timers or the dashboard changes — because this class never sees a frame, only a
 * measurement.</p>
 *
 * <h2>Concurrency</h2>
 * <p>Per-table evidence lives in a {@link ConcurrentHashMap} of short-lived
 * {@link MotionTrack} objects whose {@code offer} method is synchronised on the track itself. Alerts
 * for different tables therefore proceed in genuine parallel across Netty workers, while alerts for
 * the <em>same</em> table are serialised — which is required, since a confirmation streak is only
 * meaningful if it is updated atomically.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public class CctvMotionSignalSource implements OccupancySignalSource {

    private static final Logger LOG = LoggerFactory.getLogger(CctvMotionSignalSource.class);

    private final String sourceId;
    private final MotionConditioningPolicy policy;
    private final Clock clock;
    private final PushOccupancySignalSource delegate;
    private final boolean enabled;
    private final ConcurrentHashMap<String, MotionTrack> tracks = new ConcurrentHashMap<>();
    private final LongAdder alertsAccepted = new LongAdder();
    private final LongAdder signalsEmitted = new LongAdder();

    /**
     * Creates the adapter.
     *
     * @param sourceId   unique identifier of this transport instance; must not be blank
     * @param policy     the conditioning rule set; must not be {@code null}
     * @param clock      the clock used for debouncing and eviction; must not be {@code null}
     * @param bufferSize number of conditioned signals to buffer before applying backpressure
     * @param enabled    whether the adapter should participate in ingestion
     */
    public CctvMotionSignalSource(String sourceId,
                                  MotionConditioningPolicy policy,
                                  Clock clock,
                                  int bufferSize,
                                  boolean enabled) {
        this.sourceId = Objects.requireNonNull(sourceId, "sourceId must not be null");
        this.policy = Objects.requireNonNull(policy, "policy must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.enabled = enabled;
        this.delegate = new PushOccupancySignalSource(sourceId, SignalSourceKind.CCTV_MOTION, bufferSize);
    }

    @Override
    public String sourceId() {
        return sourceId;
    }

    @Override
    public SignalSourceKind kind() {
        return SignalSourceKind.CCTV_MOTION;
    }

    @Override
    public Flux<OccupancySignal> signals() {
        return delegate.signals();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    @Override
    public int housekeep(Instant now) {
        return evictIdleTracks(now);
    }

    /**
     * Conditions one motion alert and, when the evidence warrants it, pushes the resulting
     * occupancy signal into the ingestion pipeline.
     *
     * @param alert the alert to condition; must not be {@code null}
     * @return a {@link Mono} of the verdict, completing once any emitted signal has been accepted
     */
    public Mono<MotionIngestOutcome> accept(MotionAlert alert) {
        Objects.requireNonNull(alert, "alert must not be null");
        return Mono.defer(() -> {
            alertsAccepted.increment();
            Instant now = clock.instant();
            MotionTrack track = tracks.computeIfAbsent(alert.tableId(), key -> new MotionTrack());
            Verdict verdict = track.offer(alert, policy, now);

            if (verdict.disposition() != MotionIngestOutcome.Disposition.EMITTED) {
                LOG.trace("source={} table={} alert={} ratio={} disposition={}",
                        sourceId, alert.tableId(), alert.alertId(),
                        alert.motionRatio(), verdict.disposition());
                return Mono.just(verdict.toOutcome(alert));
            }

            OccupancySignal signal = toSignal(alert, verdict);
            signalsEmitted.increment();
            LOG.debug("source={} table={} emitted={} ratio={} confidence={} streak={}",
                    sourceId, alert.tableId(), verdict.candidate(),
                    alert.motionRatio(), alert.confidence(), verdict.winningStreak());
            return delegate.publish(signal).thenReturn(verdict.toOutcome(alert));
        });
    }

    /**
     * Evicts per-table evidence that has not been updated within the policy's retention window.
     * Invoked periodically by {@link SignalIngestionGateway} so that a camera decommissioned
     * mid-service does not leak a tracking entry for the lifetime of the JVM.
     *
     * @param now the reference instant; must not be {@code null}
     * @return the number of tracking entries evicted
     */
    public int evictIdleTracks(Instant now) {
        Objects.requireNonNull(now, "now must not be null");
        Instant cutoff = now.minus(policy.trackRetention());
        int before = tracks.size();
        tracks.values().removeIf(track -> track.isIdleSince(cutoff));
        return before - tracks.size();
    }

    /**
     * @return the number of tables for which evidence is currently being tracked
     */
    public int trackedTables() {
        return tracks.size();
    }

    /**
     * @return total alerts this adapter has conditioned since startup
     */
    public long alertsAccepted() {
        return alertsAccepted.sum();
    }

    /**
     * @return total occupancy signals this adapter has emitted since startup
     */
    public long signalsEmitted() {
        return signalsEmitted.sum();
    }

    private OccupancySignal toSignal(MotionAlert alert, Verdict verdict) {
        Map<String, String> attributes = new LinkedHashMap<>();
        attributes.put("cameraId", alert.cameraId());
        attributes.put("motionRatio", Double.toString(alert.motionRatio()));
        attributes.put("pixelDelta", Long.toString(alert.pixelDelta()));
        attributes.put("confirmationStreak", Integer.toString(verdict.winningStreak()));
        attributes.put("decisionRule", "threshold+hysteresis+streak(no-ml)");
        if (alert.zone() != null) {
            attributes.put("zone", alert.zone());
        }
        int occupancy = verdict.candidate() == SignalType.GUEST_SEATED ? alert.occupancyHint() : 0;
        return new OccupancySignal(
                alert.alertId(),
                alert.tableId(),
                verdict.candidate(),
                SignalSourceKind.CCTV_MOTION,
                alert.cameraId(),
                alert.capturedAt(),
                alert.confidence(),
                occupancy,
                Collections.unmodifiableMap(attributes));
    }

    /**
     * The conditioner's intermediate decision, carrying enough context to build either an outcome
     * or a signal without re-reading mutable track state outside the lock.
     */
    record Verdict(MotionIngestOutcome.Disposition disposition,
                           SignalType candidate,
                           int occupiedStreak,
                           int vacantStreak,
                           String narrative) {

        int winningStreak() {
            return candidate == SignalType.GUEST_SEATED ? occupiedStreak : vacantStreak;
        }

        MotionIngestOutcome toOutcome(MotionAlert alert) {
            SignalType emitted = disposition == MotionIngestOutcome.Disposition.EMITTED
                    ? candidate : null;
            return new MotionIngestOutcome(alert.alertId(), alert.tableId(), disposition,
                    emitted, occupiedStreak, vacantStreak, narrative);
        }
    }

    /**
     * Mutable, per-table evidence. Package-private purely so the unit suite can drive the rule set
     * directly without going through a sink.
     */
    static final class MotionTrack {

        /**
         * Tables are assumed vacant at genesis. Seeding the last emitted judgement with
         * {@code GUEST_DEPARTED} means a camera watching an empty table all morning never emits a
         * redundant departure that the state engine would only reject.
         */
        private SignalType lastEmitted = SignalType.GUEST_DEPARTED;
        private Instant lastEmittedAt;
        private Instant lastSeenAt = Instant.EPOCH;
        private int occupiedStreak;
        private int vacantStreak;

        synchronized Verdict offer(MotionAlert alert, MotionConditioningPolicy policy, Instant now) {
            lastSeenAt = now;

            if (alert.confidence() < policy.minConfidence()) {
                return new Verdict(MotionIngestOutcome.Disposition.LOW_CONFIDENCE, null,
                        occupiedStreak, vacantStreak,
                        "confidence %s is below the %s floor; evidence preserved"
                                .formatted(alert.confidence(), policy.minConfidence()));
            }

            SignalType candidate;
            if (alert.motionRatio() >= policy.occupiedMotionRatio()) {
                occupiedStreak = Math.min(occupiedStreak + 1, policy.confirmationFrames());
                vacantStreak = 0;
                candidate = SignalType.GUEST_SEATED;
            } else if (alert.motionRatio() <= policy.vacantMotionRatio()) {
                vacantStreak = Math.min(vacantStreak + 1, policy.confirmationFrames());
                occupiedStreak = 0;
                candidate = SignalType.GUEST_DEPARTED;
            } else {
                return new Verdict(MotionIngestOutcome.Disposition.AMBIGUOUS, null,
                        occupiedStreak, vacantStreak,
                        "motionRatio %s lies inside the hysteresis dead band (%s, %s); evidence preserved"
                                .formatted(alert.motionRatio(), policy.vacantMotionRatio(),
                                        policy.occupiedMotionRatio()));
            }

            if (candidate == lastEmitted) {
                return new Verdict(MotionIngestOutcome.Disposition.DUPLICATE_ASSERTION, candidate,
                        occupiedStreak, vacantStreak,
                        "%s has already been emitted for this table; not re-asserting"
                                .formatted(candidate));
            }

            int streak = candidate == SignalType.GUEST_SEATED ? occupiedStreak : vacantStreak;
            if (streak < policy.confirmationFrames()) {
                return new Verdict(MotionIngestOutcome.Disposition.AWAITING_CONFIRMATION, candidate,
                        occupiedStreak, vacantStreak,
                        "%s qualifying alert %s of %s; awaiting confirmation"
                                .formatted(candidate, streak, policy.confirmationFrames()));
            }

            if (lastEmittedAt != null) {
                Duration sinceLast = Duration.between(lastEmittedAt, now);
                if (sinceLast.compareTo(policy.emitDebounce()) < 0) {
                    return new Verdict(MotionIngestOutcome.Disposition.DEBOUNCED, candidate,
                            occupiedStreak, vacantStreak,
                            "last emission was %s ago; debounce window is %s"
                                    .formatted(sinceLast, policy.emitDebounce()));
                }
            }

            lastEmitted = candidate;
            lastEmittedAt = now;
            return new Verdict(MotionIngestOutcome.Disposition.EMITTED, candidate,
                    occupiedStreak, vacantStreak,
                    "%s confirmed by %s consecutive alerts; signal emitted"
                            .formatted(candidate, streak));
        }

        synchronized boolean isIdleSince(Instant cutoff) {
            return lastSeenAt.isBefore(cutoff);
        }
    }
}
