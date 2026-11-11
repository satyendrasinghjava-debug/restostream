/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.ingestion;

import com.satyendra.restostream.domain.OccupancySignal;
import com.satyendra.restostream.domain.SignalSourceKind;
import reactor.core.publisher.Flux;

import java.time.Instant;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p><strong>The extension seam of RestoStream.</strong> Every transport that can observe a table —
 * a dashboard tap today, a mobile camera tomorrow, a bank of CCTV feeds the quarter after —
 * implements this one interface and nothing else.</p>
 *
 * <p>The contract is deliberately a <em>push stream</em> rather than a request/response call. A
 * camera does not wait for an answer; it asserts what it saw and moves on. Modelling that as a
 * {@link Flux} means the state machine, the timers and the dashboard fan-out are all written once
 * and are completely indifferent to how many sources exist or what they are:</p>
 *
 * <pre>
 *   ┌──────────────────────┐
 *   │ PushOccupancySource  │ ── manual tap / mobile camera ──┐
 *   ├──────────────────────┤                                 │
 *   │ CctvMotionSource     │ ── pixel/motion alerts ─────────┤──&gt; SignalIngestionGateway
 *   ├──────────────────────┤                                 │          │
 *   │ &lt;your transport&gt;     │ ── POS, BLE, weight sensor ─────┘          ▼
 *   └──────────────────────┘                             TableStateEngine ─&gt; Redis ─&gt; STOMP
 * </pre>
 *
 * <p>To add a CCTV deployment that speaks a different protocol, publish a bean implementing this
 * interface. The gateway discovers it by type at startup and merges it into the pipeline; no
 * existing class is edited. That is the whole point of the seam.</p>
 *
 * <h2>Implementation contract</h2>
 * <ul>
 *   <li>{@link #signals()} must return a stream that is safe to subscribe to exactly once at
 *       application start and that never completes during normal operation. A completing source is
 *       treated as a retired transport and logged.</li>
 *   <li>Implementations must never block the calling thread; they are subscribed on a Netty
 *       event-loop worker.</li>
 *   <li>Implementations are responsible for their own conditioning — thresholding, debouncing,
 *       de-duplication. The gateway applies backpressure and error isolation but does not
 *       second-guess a transport's judgement.</li>
 * </ul>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public interface OccupancySignalSource {

    /**
     * Stable identifier of this concrete transport instance, used in logs, metrics tags and signal
     * provenance. Must be unique across all registered sources.
     *
     * @return a non-blank identifier, e.g. {@code cctv-motion} or {@code camera-patio-3}
     */
    String sourceId();

    /**
     * Classifies the transport so that trust weighting and per-kind policy can be applied without
     * string matching on {@link #sourceId()}.
     *
     * @return the transport classification; never {@code null}
     */
    SignalSourceKind kind();

    /**
     * The stream of observations produced by this transport.
     *
     * @return a non-completing, non-blocking stream of signals; never {@code null}
     */
    Flux<OccupancySignal> signals();

    /**
     * Allows a transport to be deployed but left dormant — for example a CCTV adapter that is wired
     * in production and switched off in a demo environment. A disabled source is never subscribed.
     *
     * @return {@code true} when this source should participate in ingestion
     */
    default boolean enabled() {
        return true;
    }

    /**
     * Periodic housekeeping hook invoked by {@link SignalIngestionGateway} on a fixed cadence.
     *
     * <p>Transports that accumulate per-table state — a motion conditioner's confirmation streaks,
     * a de-duplication window, a rate-limiter bucket — use this to bound their memory without
     * owning a scheduler of their own. The default implementation does nothing, so stateless
     * transports need not consider it.</p>
     *
     * @param now the reference instant; never {@code null}
     * @return the number of entries reclaimed, for logging and metrics
     */
    default int housekeep(Instant now) {
        return 0;
    }
}
