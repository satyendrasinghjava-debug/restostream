/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.ingestion;

import com.satyendra.restostream.domain.OccupancySignal;
import com.satyendra.restostream.domain.SignalSourceKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.Objects;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>A reusable, sink-backed {@link OccupancySignalSource} for transports that already hold a fully
 * formed {@link OccupancySignal} and simply need to hand it to the pipeline — the fire-and-forget
 * REST ingestion endpoint, inbound STOMP {@code SEND} frames, and the mobile-camera adapter.</p>
 *
 * <p>The sink is a bounded multicast buffer with {@code autoCancel} disabled, which matters for two
 * reasons: a slow consumer applies genuine backpressure instead of silently dropping observations,
 * and a transient unsubscribe by the gateway does not terminate the source for the lifetime of the
 * JVM.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public class PushOccupancySignalSource implements OccupancySignalSource {

    private static final Logger LOG = LoggerFactory.getLogger(PushOccupancySignalSource.class);

    private final String sourceId;
    private final SignalSourceKind kind;
    private final Sinks.Many<OccupancySignal> sink;

    /**
     * Creates a push source.
     *
     * @param sourceId   unique identifier of this transport instance; must not be blank
     * @param kind       transport classification; must not be {@code null}
     * @param bufferSize number of signals to buffer before applying backpressure; must be positive
     */
    public PushOccupancySignalSource(String sourceId, SignalSourceKind kind, int bufferSize) {
        this.sourceId = requireNonBlank(sourceId);
        this.kind = Objects.requireNonNull(kind, "kind must not be null");
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("bufferSize must be positive but was " + bufferSize);
        }
        this.sink = Sinks.many().multicast().onBackpressureBuffer(bufferSize, false);
    }

    @Override
    public String sourceId() {
        return sourceId;
    }

    @Override
    public SignalSourceKind kind() {
        return kind;
    }

    @Override
    public Flux<OccupancySignal> signals() {
        return sink.asFlux();
    }

    /**
     * Pushes one observation into the pipeline.
     *
     * <p>Emission failures are surfaced as an error rather than swallowed. A
     * {@code FAIL_NON_SERIALIZED} result is retried on the caller's behalf because concurrent REST
     * requests legitimately race here; every other failure is terminal and the caller is told.</p>
     *
     * @param signal the observation to publish; must not be {@code null}
     * @return a {@link Mono} completing when the signal has been accepted by the sink
     */
    public Mono<Void> publish(OccupancySignal signal) {
        Objects.requireNonNull(signal, "signal must not be null");
        return Mono.<Void>defer(() -> {
            Sinks.EmitResult result = sink.tryEmitNext(signal);
            if (result.isSuccess()) {
                return Mono.empty();
            }
            if (result == Sinks.EmitResult.FAIL_NON_SERIALIZED) {
                return Mono.error(new ConcurrentEmissionException(sourceId));
            }
            LOG.warn("source={} rejected signal={} table={} result={}",
                    sourceId, signal.signalId(), signal.tableId(), result);
            return Mono.error(new SignalRejectedException(sourceId, result));
        }).retryWhen(reactor.util.retry.Retry
                        .max(32)
                        .filter(ConcurrentEmissionException.class::isInstance))
                .onErrorMap(reactor.core.Exceptions::isRetryExhausted,
                        cause -> new SignalRejectedException(sourceId, Sinks.EmitResult.FAIL_NON_SERIALIZED));
    }

    private static String requireNonBlank(String value) {
        Objects.requireNonNull(value, "sourceId must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException("sourceId must not be blank");
        }
        return value;
    }

    /**
     * Internal marker for a losing race on the sink's serialized emission guarantee. Never escapes
     * {@link #publish(OccupancySignal)}.
     */
    private static final class ConcurrentEmissionException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private ConcurrentEmissionException(String sourceId) {
            super("concurrent emission on source " + sourceId, null, false, false);
        }
    }

    /**
     * Raised when the ingestion sink permanently refuses an observation — typically because the
     * pipeline is shutting down or the buffer is saturated by a stalled consumer.
     */
    public static final class SignalRejectedException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        private final transient Sinks.EmitResult emitResult;

        private SignalRejectedException(String sourceId, Sinks.EmitResult emitResult) {
            super("ingestion source '%s' refused the signal (%s)".formatted(sourceId, emitResult));
            this.emitResult = emitResult;
        }

        /**
         * @return the underlying sink result that caused the refusal
         */
        public Sinks.EmitResult emitResult() {
            return emitResult;
        }
    }
}
