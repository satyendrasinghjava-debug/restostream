/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.stomp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.satyendra.restostream.broadcast.BroadcastEnvelope;
import com.satyendra.restostream.broadcast.DashboardBroadcaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.util.retry.Retry;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Bridges the transport-agnostic {@link DashboardBroadcaster} to the STOMP broker: subscribes to
 * the envelope stream, serialises each payload <strong>once</strong>, and hands the resulting bytes
 * to the {@link StompSessionRegistry} for every destination the envelope is addressed to.</p>
 *
 * <p>Serialising once per envelope rather than once per subscriber is the whole reason this relay
 * exists as a separate component. With forty dashboards attached and a table transitioning, the
 * naive design runs forty JSON serialisations of an identical object on an event-loop thread. Here
 * it runs one, and the forty deliveries are array copies.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@Component
public class StompBroadcastRelay implements SmartLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(StompBroadcastRelay.class);
    private static final int LIFECYCLE_PHASE = Integer.MAX_VALUE - 3_000;

    private final DashboardBroadcaster broadcaster;
    private final StompSessionRegistry registry;
    private final ObjectMapper objectMapper;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile Disposable subscription;

    /**
     * Creates the relay.
     *
     * @param broadcaster  the source of broadcast envelopes
     * @param registry     the STOMP routing table
     * @param objectMapper the application {@link ObjectMapper}
     */
    public StompBroadcastRelay(DashboardBroadcaster broadcaster,
                               StompSessionRegistry registry,
                               ObjectMapper objectMapper) {
        this.broadcaster = Objects.requireNonNull(broadcaster, "broadcaster must not be null");
        this.registry = Objects.requireNonNull(registry, "registry must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        LOG.info("STOMP broadcast relay attached to the dashboard fan-out");
        subscription = broadcaster.envelopes()
                .doOnNext(this::relay)
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(200))
                        .maxBackoff(Duration.ofSeconds(5))
                        .doBeforeRetry(retry -> LOG.error(
                                "STOMP broadcast relay failed and will resubscribe (attempt {})",
                                retry.totalRetries() + 1, retry.failure())))
                .subscribe(
                        envelope -> { },
                        error -> LOG.error("STOMP broadcast relay terminated unrecoverably", error));
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        Disposable current = subscription;
        if (current != null) {
            current.dispose();
            subscription = null;
        }
        LOG.info("STOMP broadcast relay detached");
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return LIFECYCLE_PHASE;
    }

    private void relay(BroadcastEnvelope envelope) {
        if (registry.sessionCount() == 0) {
            return;
        }
        byte[] body;
        try {
            body = objectMapper.writeValueAsBytes(envelope.payload());
        } catch (com.fasterxml.jackson.core.JsonProcessingException ex) {
            LOG.error("could not serialise a {} broadcast payload for {}",
                    envelope.kind(), envelope.destinations(), ex);
            return;
        }
        // One call for the whole envelope, so a subscription matching more than one of its
        // destinations still receives exactly one frame.
        int delivered = registry.broadcastAll(envelope.destinations(), envelope.kind(), body);
        if (LOG.isTraceEnabled()) {
            LOG.trace("relayed {} ({} bytes) to {} subscription(s) across {} destination(s)",
                    envelope.kind(), body.length, delivered, envelope.destinations().size());
        }
    }

    /**
     * Serialises and relays one envelope synchronously. Exposed so that unit tests can assert
     * routing without driving the whole reactive subscription.
     *
     * @param envelope the envelope to relay; must not be {@code null}
     */
    public void relayNow(BroadcastEnvelope envelope) {
        relay(Objects.requireNonNull(envelope, "envelope must not be null"));
    }

    /**
     * @return the UTF-8 charset used for all STOMP payload bodies
     */
    public static java.nio.charset.Charset payloadCharset() {
        return StandardCharsets.UTF_8;
    }
}
