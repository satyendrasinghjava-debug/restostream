/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.stomp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>One connected dashboard: its transport, its outbound frame sink, its inbound frame
 * accumulator, its subscriptions and its negotiated heart-beat settings.</p>
 *
 * <h2>Why an outbound sink instead of writing to the session directly</h2>
 * <p>{@link WebSocketSession#send} may be called exactly once per session in the reactive API — it
 * takes a {@code Publisher} of messages and is subscribed for the session's lifetime. Every other
 * component that wants to push a frame therefore needs a way to feed that publisher after the fact,
 * which is what the sink provides. It is a {@code unicast().onBackpressureBuffer} with a bounded
 * queue: a dashboard on a saturated connection accumulates frames up to the bound and is then
 * disconnected, rather than being allowed to pin broker memory indefinitely — the standard
 * slow-consumer defence for a fan-out broker.</p>
 *
 * <h2>Destination matching</h2>
 * <p>Subscriptions may be exact ({@code /topic/tables/T-01}) or Ant-style patterns
 * ({@code /topic/tables/**}), which lets one dashboard tile subscribe to a single table while a
 * floor-wide view subscribes to all of them with one frame.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public final class StompClientSession {

    private static final Logger LOG = LoggerFactory.getLogger(StompClientSession.class);
    private static final AntPathMatcher MATCHER = new AntPathMatcher();

    private final String sessionId;
    private final WebSocketSession transport;
    private final Sinks.Many<WebSocketMessage> outbound;
    private final StompFrameAccumulator accumulator;
    private final Map<String, String> subscriptions = new ConcurrentHashMap<>();
    private final AtomicLong messageIds = new AtomicLong();
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicBoolean closing = new AtomicBoolean(false);

    private volatile Duration clientHeartbeatSend = Duration.ZERO;
    private volatile Duration clientHeartbeatReceive = Duration.ZERO;
    private volatile String negotiatedVersion = "1.2";

    /**
     * Creates a session wrapper around a freshly accepted WebSocket.
     *
     * @param transport          the underlying reactive WebSocket session; must not be {@code null}
     * @param outboundBufferSize bounded depth of the outbound frame queue; must be positive
     */
    public StompClientSession(WebSocketSession transport, int outboundBufferSize) {
        this.transport = Objects.requireNonNull(transport, "transport must not be null");
        if (outboundBufferSize <= 0) {
            throw new IllegalArgumentException(
                    "outboundBufferSize must be positive but was " + outboundBufferSize);
        }
        this.sessionId = transport.getId();
        this.outbound = Sinks.many().unicast()
                .onBackpressureBuffer(new java.util.concurrent.ArrayBlockingQueue<>(outboundBufferSize));
        this.accumulator = new StompFrameAccumulator();
    }

    /**
     * @return the broker's identifier for this session
     */
    public String sessionId() {
        return sessionId;
    }

    /**
     * @return the inbound frame accumulator confined to this session
     */
    public StompFrameAccumulator accumulator() {
        return accumulator;
    }

    /**
     * @return the stream of outbound WebSocket messages to be handed to {@link WebSocketSession#send}
     */
    public Flux<WebSocketMessage> outbound() {
        return outbound.asFlux();
    }

    /**
     * @return {@code true} once the client has completed the STOMP handshake
     */
    public boolean isConnected() {
        return connected.get();
    }

    /**
     * @return the protocol version agreed during the handshake
     */
    public String negotiatedVersion() {
        return negotiatedVersion;
    }

    /**
     * @return the interval at which this client promises to send heart-beats, zero when disabled
     */
    public Duration clientHeartbeatSend() {
        return clientHeartbeatSend;
    }

    /**
     * @return the interval at which this client expects to receive heart-beats, zero when disabled
     */
    public Duration clientHeartbeatReceive() {
        return clientHeartbeatReceive;
    }

    /**
     * Records a completed handshake.
     *
     * @param version          the negotiated protocol version
     * @param clientSend       interval at which the client will send heart-beats
     * @param clientReceive    interval at which the client wants to receive heart-beats
     * @return {@code true} when this call completed the handshake, {@code false} if already connected
     */
    public boolean markConnected(String version, Duration clientSend, Duration clientReceive) {
        this.negotiatedVersion = Objects.requireNonNull(version, "version must not be null");
        this.clientHeartbeatSend = Objects.requireNonNull(clientSend, "clientSend must not be null");
        this.clientHeartbeatReceive =
                Objects.requireNonNull(clientReceive, "clientReceive must not be null");
        return connected.compareAndSet(false, true);
    }

    /**
     * Registers a subscription.
     *
     * @param subscriptionId the client's subscription identifier; must not be {@code null}
     * @param destination    the destination or Ant pattern subscribed to; must not be {@code null}
     * @return {@code false} when the identifier was already in use, as the specification forbids
     */
    public boolean addSubscription(String subscriptionId, String destination) {
        Objects.requireNonNull(subscriptionId, "subscriptionId must not be null");
        Objects.requireNonNull(destination, "destination must not be null");
        return subscriptions.putIfAbsent(subscriptionId, destination) == null;
    }

    /**
     * Removes a subscription.
     *
     * @param subscriptionId the client's subscription identifier; must not be {@code null}
     * @return the destination that was subscribed, or {@code null} when the identifier was unknown
     */
    public String removeSubscription(String subscriptionId) {
        Objects.requireNonNull(subscriptionId, "subscriptionId must not be null");
        return subscriptions.remove(subscriptionId);
    }

    /**
     * @return an immutable view of this session's subscriptions, keyed by subscription identifier
     */
    public Map<String, String> subscriptions() {
        return Map.copyOf(subscriptions);
    }

    /**
     * Finds the subscription identifiers that should receive a message published to a destination.
     *
     * @param destination the destination being published to; must not be {@code null}
     * @return the matching subscription identifiers, possibly empty
     */
    public List<String> subscriptionsMatching(String destination) {
        Objects.requireNonNull(destination, "destination must not be null");
        List<String> matches = new ArrayList<>(2);
        subscriptions.forEach((subscriptionId, subscribed) -> {
            if (matches(subscribed, destination)) {
                matches.add(subscriptionId);
            }
        });
        return matches;
    }

    /**
     * Tests whether a subscribed destination matches a published destination.
     *
     * @param subscribed the destination or Ant pattern the client subscribed to
     * @param published  the destination being published to
     * @return {@code true} when the subscription should receive the message
     */
    public static boolean matches(String subscribed, String published) {
        if (subscribed.equals(published)) {
            return true;
        }
        return MATCHER.isPattern(subscribed) && MATCHER.match(subscribed, published);
    }

    /**
     * Tests whether a destination string is an Ant-style pattern rather than a literal.
     *
     * @param destination the destination to inspect; must not be {@code null}
     * @return {@code true} when the destination contains pattern syntax
     */
    public static boolean isPattern(String destination) {
        Objects.requireNonNull(destination, "destination must not be null");
        return MATCHER.isPattern(destination);
    }

    /**
     * @return the next per-session message identifier
     */
    public String nextMessageId() {
        return sessionId + '-' + messageIds.incrementAndGet();
    }

    /**
     * Queues an already-encoded STOMP frame for delivery.
     *
     * @param frame the encoded frame; must not be {@code null}
     * @return {@code true} when the frame was accepted by the outbound buffer
     */
    public boolean send(byte[] frame) {
        Objects.requireNonNull(frame, "frame must not be null");
        if (closing.get()) {
            return false;
        }
        WebSocketMessage message =
                transport.textMessage(new String(frame, StandardCharsets.UTF_8));
        Sinks.EmitResult result = outbound.tryEmitNext(message);
        if (!result.isSuccess()) {
            LOG.warn("session={} could not accept an outbound frame ({}); disconnecting the client",
                    sessionId, result);
            closeQuietly();
            return false;
        }
        return true;
    }

    /**
     * Queues a heart-beat.
     *
     * @return {@code true} when the heart-beat was accepted
     */
    public boolean sendHeartbeat() {
        return send(StompFrameWriter.heartbeat());
    }

    /**
     * Sends an {@code ERROR} frame and then terminates the session, as STOMP 1.2 requires.
     *
     * @param shortDescription one-line summary for the {@code message} header
     * @param detail           full explanation for the frame body; may be {@code null}
     * @param receiptId        the client receipt this error answers; may be {@code null}
     */
    public void sendErrorAndClose(String shortDescription, String detail, String receiptId) {
        if (closing.get()) {
            return;
        }
        send(StompFrameWriter.error(shortDescription, detail, receiptId));
        closeQuietly();
    }

    /**
     * Completes the outbound stream, which in turn completes {@link WebSocketSession#send} and
     * closes the transport.
     */
    public void closeQuietly() {
        if (closing.compareAndSet(false, true)) {
            outbound.tryEmitComplete();
            accumulator.reset();
        }
    }

    /**
     * @return {@code true} once this session has begun closing
     */
    public boolean isClosing() {
        return closing.get();
    }
}
