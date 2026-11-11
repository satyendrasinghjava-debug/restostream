/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>A real STOMP 1.2 client for the integration suite, speaking the protocol over a genuine
 * WebSocket connection to a running RestoStream instance.</p>
 *
 * <p>An independent client implementation is used on purpose. If the tests reused the server's own
 * frame writer and Spring's decoder, a symmetric bug — a header the server emits and the server
 * alone understands — would pass unnoticed. Parsing the bytes here with separate code means the
 * integration suite genuinely asserts wire compatibility, which is what a browser running
 * {@code stomp.js} will require.</p>
 *
 * <p>Frames are published through a replaying sink, so a test can connect, act, and only then look
 * for the frame it expected without racing the server.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public final class StompTestClient implements AutoCloseable {

    private static final char NULL = '\u0000';
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(15);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Sinks.Many<String> outbound = Sinks.many().unicast().onBackpressureBuffer();
    private final Sinks.Many<Frame> frames = Sinks.many().replay().all();
    private final AtomicInteger subscriptionIds = new AtomicInteger();

    private volatile Disposable session;
    private volatile String buffer = "";

    private StompTestClient() {
    }

    /**
     * Opens a WebSocket connection, performs the STOMP handshake and returns once the server's
     * {@code CONNECTED} frame has arrived.
     *
     * @param url the {@code ws://} endpoint; must not be {@code null}
     * @return a connected client
     */
    public static StompTestClient connect(String url) {
        Objects.requireNonNull(url, "url must not be null");
        StompTestClient client = new StompTestClient();
        ReactorNettyWebSocketClient transport = new ReactorNettyWebSocketClient();

        WebSocketHandler handler = new WebSocketHandler() {
            @Override
            public List<String> getSubProtocols() {
                return List.of("v12.stomp");
            }

            @Override
            public Mono<Void> handle(WebSocketSession socket) {
                Mono<Void> send = socket.send(client.outbound.asFlux().map(socket::textMessage));
                Mono<Void> receive = socket.receive()
                        .map(message -> message.getPayloadAsText(StandardCharsets.UTF_8))
                        .doOnNext(client::onPayload)
                        .then();
                return send.and(receive);
            }
        };

        client.session = transport.execute(URI.create(url), handler).subscribe();
        client.write("CONNECT", Map.of(
                "accept-version", "1.2",
                "host", "localhost",
                "heart-beat", "10000,10000"), null);
        client.awaitFrame(frame -> "CONNECTED".equals(frame.command()), DEFAULT_TIMEOUT);
        return client;
    }

    /**
     * Subscribes to a destination and waits for the server's receipt, so that a subsequent action
     * cannot race the subscription being registered.
     *
     * @param destination the destination or Ant pattern to subscribe to
     * @return the subscription identifier that was used
     */
    public String subscribe(String destination) {
        String id = "sub-" + subscriptionIds.incrementAndGet();
        String receipt = "r-" + id;
        write("SUBSCRIBE", Map.of("id", id, "destination", destination,
                "ack", "auto", "receipt", receipt), null);
        awaitFrame(frame -> "RECEIPT".equals(frame.command())
                && receipt.equals(frame.headers().get("receipt-id")), DEFAULT_TIMEOUT);
        return id;
    }

    /**
     * Sends a JSON command to an application destination.
     *
     * @param destination the application destination
     * @param payload     the object to serialise as the frame body
     */
    public void send(String destination, Object payload) {
        try {
            write("SEND", Map.of("destination", destination,
                            "content-type", "application/json;charset=utf-8"),
                    objectMapper.writeValueAsString(payload));
        } catch (IOException ex) {
            throw new IllegalStateException("could not serialise the command payload", ex);
        }
    }

    /**
     * @return every frame received so far, replayed from the start of the connection
     */
    public Flux<Frame> frames() {
        return frames.asFlux();
    }

    /**
     * Waits for the first frame matching a predicate, including frames that arrived before this
     * call.
     *
     * @param predicate the frame to wait for
     * @param timeout   how long to wait
     * @return the matching frame
     */
    public Frame awaitFrame(java.util.function.Predicate<Frame> predicate, Duration timeout) {
        Frame frame = frames.asFlux().filter(predicate).next().block(timeout);
        if (frame == null) {
            throw new AssertionError("no frame matched within " + timeout);
        }
        return frame;
    }

    /**
     * Waits for the first {@code MESSAGE} frame on a destination carrying the given content kind.
     *
     * @param destination the destination the frame was published to
     * @param contentKind the value of the {@code content-kind} header
     * @return the matching frame
     */
    public Frame awaitMessage(String destination, String contentKind) {
        return awaitFrame(frame -> "MESSAGE".equals(frame.command())
                && destination.equals(frame.headers().get("destination"))
                && contentKind.equals(frame.headers().get("content-kind")), DEFAULT_TIMEOUT);
    }

    /**
     * Waits for the first {@code MESSAGE} frame on a destination whose body matches a predicate.
     *
     * @param destination the destination the frame was published to
     * @param bodyMatches the body predicate
     * @param timeout     how long to wait
     * @return the matching frame
     */
    public Frame awaitMessage(String destination,
                              java.util.function.Predicate<String> bodyMatches,
                              Duration timeout) {
        return awaitFrame(frame -> "MESSAGE".equals(frame.command())
                && destination.equals(frame.headers().get("destination"))
                && bodyMatches.test(frame.body()), timeout);
    }

    /**
     * Sends a raw frame, for tests that deliberately violate the protocol.
     *
     * @param command the STOMP command
     * @param headers the headers
     * @param body    the body, or {@code null}
     */
    public void write(String command, Map<String, String> headers, String body) {
        StringBuilder frame = new StringBuilder(command).append('\n');
        headers.forEach((name, value) -> frame.append(escape(name)).append(':')
                .append(escape(value)).append('\n'));
        frame.append('\n').append(body == null ? "" : body).append(NULL);
        outbound.tryEmitNext(frame.toString());
    }

    @Override
    public void close() {
        write("DISCONNECT", Map.of(), null);
        outbound.tryEmitComplete();
        Disposable current = session;
        if (current != null) {
            current.dispose();
        }
    }

    private synchronized void onPayload(String payload) {
        buffer += payload;
        List<Frame> parsed = new ArrayList<>();
        String rest = buffer;
        while (true) {
            while (rest.startsWith("\n") || rest.startsWith("\r\n")) {
                rest = rest.startsWith("\r\n") ? rest.substring(2) : rest.substring(1);
            }
            int end = rest.indexOf(NULL);
            if (end < 0) {
                break;
            }
            String raw = rest.substring(0, end);
            rest = rest.substring(end + 1);
            if (!raw.isBlank()) {
                parsed.add(parse(raw));
            }
        }
        buffer = rest;
        parsed.forEach(frames::tryEmitNext);
    }

    private static Frame parse(String raw) {
        int split = raw.indexOf("\n\n");
        String head = split < 0 ? raw : raw.substring(0, split);
        String body = split < 0 ? "" : raw.substring(split + 2);
        String[] lines = head.split("\n");
        Map<String, String> headers = new HashMap<>();
        for (int index = 1; index < lines.length; index++) {
            int colon = lines[index].indexOf(':');
            if (colon > 0) {
                String name = unescape(lines[index].substring(0, colon));
                headers.putIfAbsent(name, unescape(lines[index].substring(colon + 1)));
            }
        }
        return new Frame(lines[0].trim(), headers, body);
    }

    private static String escape(String raw) {
        return raw.replace("\\", "\\\\").replace(":", "\\c")
                .replace("\n", "\\n").replace("\r", "\\r");
    }

    private static String unescape(String raw) {
        return raw.replace("\\c", ":").replace("\\n", "\n")
                .replace("\\r", "\r").replace("\\\\", "\\");
    }

    /**
     * One decoded STOMP frame.
     *
     * @param command the STOMP command
     * @param headers the decoded headers
     * @param body    the frame body, possibly empty
     */
    public record Frame(String command, Map<String, String> headers, String body) {

        /**
         * Parses the frame body as JSON into the requested type.
         *
         * @param mapper the mapper to use
         * @param type   the target type
         * @param <T>    the target type
         * @return the deserialised body
         */
        public <T> T bodyAs(ObjectMapper mapper, Class<T> type) {
            try {
                return mapper.readValue(body, type);
            } catch (IOException ex) {
                throw new IllegalStateException(
                        "frame body is not valid JSON for " + type.getSimpleName() + ": " + body, ex);
            }
        }
    }
}
