/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.stomp;

import com.satyendra.restostream.config.RestoStreamProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.messaging.Message;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>A STOMP 1.2 broker implemented directly on the reactive WebFlux WebSocket stack.</p>
 *
 * <h2>Why this class exists at all</h2>
 * <p>Spring's familiar STOMP support — {@code @EnableWebSocketMessageBroker},
 * {@code SimpMessagingTemplate}, {@code @MessageMapping} — is built on the Servlet stack and does
 * not run on WebFlux. A system whose requirement is a reactive state manager <em>and</em> a STOMP
 * broadcaster therefore has to choose: abandon WebFlux and accept a thread-per-request stack, or
 * implement the protocol on the reactive stack. RestoStream chooses the latter, because a live
 * dashboard broadcaster is precisely the workload where the reactive model pays off — thousands of
 * mostly-idle connections being fanned out to, on a handful of event-loop threads, with no thread
 * parked per dashboard.</p>
 *
 * <p>The protocol is implemented once, carefully, and reuses Spring's own production
 * {@link org.springframework.messaging.simp.stomp.StompDecoder} for the adversarial inbound
 * direction. The result is wire-compatible with the standard JavaScript {@code stomp.js} client and
 * with Spring's own {@code ReactorNettyTcpStompClient}-style tooling, which is what the integration
 * suite connects with.</p>
 *
 * <h2>Session anatomy</h2>
 * <p>Each accepted socket produces one {@link StompClientSession} and three concurrent concerns,
 * composed so that the session ends cleanly when any of them does:</p>
 * <ul>
 *   <li><strong>inbound</strong> — decode and dispatch client frames, with an idle timeout;</li>
 *   <li><strong>outbound</strong> — drain the session's frame sink into the socket;</li>
 *   <li><strong>heart-beat</strong> — emit a bare end-of-line at the negotiated cadence.</li>
 * </ul>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@Component
public class ReactiveStompWebSocketHandler implements WebSocketHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ReactiveStompWebSocketHandler.class);

    /** Sub-protocols advertised during the WebSocket handshake, most preferred first. */
    private static final List<String> SUB_PROTOCOLS = List.of("v12.stomp", "v11.stomp", "v10.stomp");

    /** Protocol versions this broker implements. */
    private static final Set<String> SUPPORTED_VERSIONS = Set.of("1.0", "1.1", "1.2");

    private static final String SERVER_NAME = "RestoStream/1.0.0";

    private final StompSessionRegistry registry;
    private final StompApplicationGateway applicationGateway;
    private final RestoStreamProperties.Stomp config;

    /**
     * Creates the broker.
     *
     * @param registry           the routing table sessions are registered in
     * @param applicationGateway the application boundary subscriptions and commands are handed to
     * @param properties         application configuration
     */
    public ReactiveStompWebSocketHandler(StompSessionRegistry registry,
                                         StompApplicationGateway applicationGateway,
                                         RestoStreamProperties properties) {
        this.registry = Objects.requireNonNull(registry, "registry must not be null");
        this.applicationGateway =
                Objects.requireNonNull(applicationGateway, "applicationGateway must not be null");
        this.config = Objects.requireNonNull(properties, "properties must not be null").stomp();
    }

    @Override
    public List<String> getSubProtocols() {
        return SUB_PROTOCOLS;
    }

    @Override
    public Mono<Void> handle(WebSocketSession transport) {
        StompClientSession client = new StompClientSession(transport, config.outboundBufferSize());
        registry.register(client);
        Disposable heartbeats = startHeartbeats(client);

        Mono<Void> inbound = transport.receive()
                .timeout(config.sessionIdleTimeout())
                .concatMap(message -> onWebSocketMessage(client, message))
                .then()
                .onErrorResume(error -> onTransportError(client, error))
                // Once the client stops sending, stop sending to it. Completing the outbound sink
                // here is what allows `and` below to complete: without it, a client that simply
                // vanished would leave its session registered — and still being broadcast to — for
                // the remaining life of the process.
                .doFinally(signal -> client.closeQuietly());

        Mono<Void> outbound = transport.send(client.outbound());

        return outbound.and(inbound)
                .doFinally(signal -> {
                    heartbeats.dispose();
                    registry.unregister(client.sessionId());
                });
    }

    private Disposable startHeartbeats(StompClientSession client) {
        Duration cadence = config.heartbeatSend();
        if (cadence.isZero() || cadence.isNegative()) {
            return Disposables.disposed();
        }
        return Flux.interval(cadence, cadence, Schedulers.parallel())
                .onBackpressureDrop()
                .doOnNext(tick -> {
                    // Only a client that asked to receive heart-beats gets them; a client that
                    // negotiated 0 would treat an unexpected end-of-line as a protocol error.
                    if (client.isConnected()
                            && !client.isClosing()
                            && !client.clientHeartbeatReceive().isZero()) {
                        client.sendHeartbeat();
                    }
                })
                .subscribe();
    }

    private Mono<Void> onTransportError(StompClientSession client, Throwable error) {
        if (error instanceof TimeoutException) {
            LOG.info("STOMP session {} exceeded the {} idle timeout and will be closed",
                    client.sessionId(), config.sessionIdleTimeout());
            client.sendErrorAndClose("session idle timeout",
                    "no frame or heart-beat was received within " + config.sessionIdleTimeout(), null);
            return Mono.empty();
        }
        LOG.warn("STOMP session {} failed on the inbound stream", client.sessionId(), error);
        client.closeQuietly();
        return Mono.empty();
    }

    private Mono<Void> onWebSocketMessage(StompClientSession client, WebSocketMessage message) {
        WebSocketMessage.Type type = message.getType();
        if (type != WebSocketMessage.Type.TEXT && type != WebSocketMessage.Type.BINARY) {
            // PING and PONG are handled by the WebSocket container itself.
            return Mono.empty();
        }
        // The payload buffer is read eagerly here, before any Mono is assembled, because the
        // container may release it as soon as this method returns.
        byte[] raw = readFully(message.getPayload());
        List<Message<byte[]>> frames;
        try {
            frames = client.accumulator().accumulate(raw);
        } catch (StompProtocolException ex) {
            LOG.warn("STOMP session {} sent an undecodable frame: {}",
                    client.sessionId(), ex.getMessage());
            client.sendErrorAndClose("malformed frame", ex.getMessage(), null);
            return Mono.empty();
        }
        if (frames.isEmpty()) {
            return Mono.empty();
        }
        return Flux.fromIterable(frames)
                .concatMap(frame -> onFrame(client, frame))
                .then();
    }

    private static byte[] readFully(DataBuffer payload) {
        int count = payload.readableByteCount();
        byte[] bytes = new byte[count];
        if (count > 0) {
            payload.read(bytes);
        }
        return bytes;
    }

    private Mono<Void> onFrame(StompClientSession client, Message<byte[]> frame) {
        StompHeaderAccessor accessor = StompHeaderAccessor.wrap(frame);
        StompCommand command = accessor.getCommand();
        if (command == null) {
            // A bare end-of-line: the client's heart-beat. Receiving it is the point; the idle
            // timeout upstream has already been reset by its arrival.
            return Mono.empty();
        }
        String receiptId = accessor.getFirstNativeHeader("receipt");

        if (!client.isConnected() && command != StompCommand.CONNECT && command != StompCommand.STOMP) {
            client.sendErrorAndClose("not connected",
                    "a " + command + " frame was received before CONNECT", receiptId);
            return Mono.empty();
        }

        return switch (command) {
            case CONNECT, STOMP -> onConnect(client, accessor, receiptId);
            case SUBSCRIBE -> onSubscribe(client, accessor, receiptId);
            case UNSUBSCRIBE -> onUnsubscribe(client, accessor, receiptId);
            case SEND -> onSend(client, accessor, frame.getPayload(), receiptId);
            case DISCONNECT -> onDisconnect(client, receiptId);
            case ACK, NACK -> {
                // The broker never sends an ack header, so there is nothing to acknowledge. The
                // frame is accepted and receipted rather than treated as an error.
                acknowledge(client, receiptId);
                yield Mono.empty();
            }
            case BEGIN, COMMIT, ABORT -> {
                client.sendErrorAndClose("transactions are not supported",
                        "RestoStream broadcasts idempotent state; STOMP transactions are not implemented",
                        receiptId);
                yield Mono.empty();
            }
            default -> {
                client.sendErrorAndClose("unexpected frame",
                        command + " is a server-to-client frame and must not be sent by a client",
                        receiptId);
                yield Mono.empty();
            }
        };
    }

    private Mono<Void> onConnect(StompClientSession client,
                                 StompHeaderAccessor accessor,
                                 String receiptId) {
        if (client.isConnected()) {
            client.sendErrorAndClose("already connected",
                    "this session has already completed the STOMP handshake", receiptId);
            return Mono.empty();
        }
        String acceptVersion = accessor.getFirstNativeHeader("accept-version");
        String negotiated = negotiateVersion(acceptVersion);
        if (negotiated == null) {
            client.sendErrorAndClose("unsupported protocol version",
                    "supported versions are " + SUPPORTED_VERSIONS + " but the client offered '"
                            + acceptVersion + "'", receiptId);
            return Mono.empty();
        }

        Duration[] clientHeartbeat = parseHeartbeat(accessor.getFirstNativeHeader("heart-beat"));
        client.markConnected(negotiated, clientHeartbeat[0], clientHeartbeat[1]);

        long serverSendMs = effective(config.heartbeatSend(), clientHeartbeat[1]).toMillis();
        long serverExpectMs = effective(config.heartbeatReceive(), clientHeartbeat[0]).toMillis();

        client.send(StompFrameWriter.connected(
                negotiated, client.sessionId(), SERVER_NAME, serverSendMs, serverExpectMs));
        acknowledge(client, receiptId);
        LOG.debug("STOMP session {} completed the handshake on version {} (heart-beat {},{})",
                client.sessionId(), negotiated, serverSendMs, serverExpectMs);
        return Mono.empty();
    }

    private Mono<Void> onSubscribe(StompClientSession client,
                                   StompHeaderAccessor accessor,
                                   String receiptId) {
        String subscriptionId = accessor.getFirstNativeHeader("id");
        String destination = accessor.getFirstNativeHeader("destination");
        if (subscriptionId == null || subscriptionId.isBlank()) {
            client.sendErrorAndClose("missing id header",
                    "a SUBSCRIBE frame must carry a unique 'id' header", receiptId);
            return Mono.empty();
        }
        if (destination == null || destination.isBlank()) {
            client.sendErrorAndClose("missing destination header",
                    "a SUBSCRIBE frame must carry a 'destination' header", receiptId);
            return Mono.empty();
        }
        if (!destination.startsWith(config.topicPrefix())) {
            client.sendErrorAndClose("illegal destination",
                    "subscriptions are only accepted under '" + config.topicPrefix()
                            + "' but '" + destination + "' was requested", receiptId);
            return Mono.empty();
        }
        if (!client.addSubscription(subscriptionId, destination)) {
            client.sendErrorAndClose("duplicate subscription id",
                    "subscription id '" + subscriptionId + "' is already in use on this session",
                    receiptId);
            return Mono.empty();
        }
        acknowledge(client, receiptId);
        LOG.debug("STOMP session {} subscribed '{}' to {}",
                client.sessionId(), subscriptionId, destination);
        return applicationGateway.onSubscribed(client, subscriptionId, destination)
                .onErrorResume(error -> {
                    LOG.error("failed to deliver initial state to session {} for {}",
                            client.sessionId(), destination, error);
                    return Mono.empty();
                });
    }

    private Mono<Void> onUnsubscribe(StompClientSession client,
                                     StompHeaderAccessor accessor,
                                     String receiptId) {
        String subscriptionId = accessor.getFirstNativeHeader("id");
        if (subscriptionId == null || subscriptionId.isBlank()) {
            client.sendErrorAndClose("missing id header",
                    "an UNSUBSCRIBE frame must carry the 'id' header of the subscription to remove",
                    receiptId);
            return Mono.empty();
        }
        String removed = client.removeSubscription(subscriptionId);
        if (removed == null) {
            LOG.debug("STOMP session {} unsubscribed unknown id '{}'; ignoring",
                    client.sessionId(), subscriptionId);
        }
        acknowledge(client, receiptId);
        return Mono.empty();
    }

    private Mono<Void> onSend(StompClientSession client,
                              StompHeaderAccessor accessor,
                              byte[] body,
                              String receiptId) {
        String destination = accessor.getFirstNativeHeader("destination");
        if (destination == null || destination.isBlank()) {
            client.sendErrorAndClose("missing destination header",
                    "a SEND frame must carry a 'destination' header", receiptId);
            return Mono.empty();
        }
        if (!destination.startsWith(config.applicationPrefix())) {
            client.sendErrorAndClose("illegal destination",
                    "commands are only accepted under '" + config.applicationPrefix()
                            + "' but '" + destination + "' was addressed", receiptId);
            return Mono.empty();
        }
        acknowledge(client, receiptId);
        return applicationGateway.onSend(client, destination, body)
                .onErrorResume(error -> {
                    LOG.error("session {} command to {} failed",
                            client.sessionId(), destination, error);
                    return Mono.empty();
                });
    }

    private Mono<Void> onDisconnect(StompClientSession client, String receiptId) {
        acknowledge(client, receiptId);
        LOG.debug("STOMP session {} requested disconnect", client.sessionId());
        client.closeQuietly();
        return Mono.empty();
    }

    private static void acknowledge(StompClientSession client, String receiptId) {
        if (receiptId != null && !receiptId.isBlank()) {
            client.send(StompFrameWriter.receipt(receiptId));
        }
    }

    /**
     * Chooses the highest mutually supported protocol version.
     *
     * @param acceptVersion the client's {@code accept-version} header; may be {@code null}
     * @return the negotiated version, or {@code null} when there is no overlap
     */
    static String negotiateVersion(String acceptVersion) {
        if (acceptVersion == null || acceptVersion.isBlank()) {
            // STOMP 1.0 clients omit the header entirely.
            return "1.0";
        }
        String best = null;
        for (String candidate : acceptVersion.split(",")) {
            String offered = candidate.trim();
            if (SUPPORTED_VERSIONS.contains(offered) && (best == null || offered.compareTo(best) > 0)) {
                best = offered;
            }
        }
        return best;
    }

    /**
     * Parses a {@code heart-beat} header into the client's send and receive intervals.
     *
     * @param header the header value, e.g. {@code 10000,10000}; may be {@code null}
     * @return a two-element array of {@code [clientSend, clientReceive]}, zero meaning disabled
     */
    static Duration[] parseHeartbeat(String header) {
        Duration[] parsed = {Duration.ZERO, Duration.ZERO};
        if (header == null || header.isBlank()) {
            return parsed;
        }
        String[] parts = header.split(",");
        if (parts.length != 2) {
            return parsed;
        }
        try {
            long send = Math.max(0L, Long.parseLong(parts[0].trim()));
            long receive = Math.max(0L, Long.parseLong(parts[1].trim()));
            parsed[0] = Duration.ofMillis(send);
            parsed[1] = Duration.ofMillis(receive);
        } catch (NumberFormatException ex) {
            // A malformed heart-beat header disables heart-beating rather than failing the
            // handshake; the idle timeout still protects the broker from abandoned sessions.
            return new Duration[]{Duration.ZERO, Duration.ZERO};
        }
        return parsed;
    }

    /**
     * Applies the STOMP heart-beat negotiation rule: if either side offers zero the direction is
     * disabled, otherwise the slower of the two cadences wins.
     *
     * @param ours   this broker's configured interval
     * @param theirs the client's requested interval
     * @return the effective interval for that direction
     */
    static Duration effective(Duration ours, Duration theirs) {
        if (ours.isZero() || ours.isNegative() || theirs.isZero() || theirs.isNegative()) {
            return Duration.ZERO;
        }
        return ours.compareTo(theirs) >= 0 ? ours : theirs;
    }
}
