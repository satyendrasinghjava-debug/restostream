/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.support;

import org.reactivestreams.Publisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.HandshakeInfo;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>An in-memory {@link WebSocketSession} for driving the STOMP broker in unit tests.</p>
 *
 * <p>A hand-written double is used in preference to a mock for two reasons. The broker builds its
 * outbound frames through the session's own {@code textMessage(String)} factory, so a mock would
 * have to stub buffer allocation — fragile, and less readable than implementing the interface
 * honestly. More importantly, the broker tests need to assert on <em>what was actually written to
 * the wire</em> and to push arbitrary bytes inbound on demand; this double records the former and
 * exposes the latter, which no amount of stubbing provides.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public final class FakeWebSocketSession implements WebSocketSession {

    private final String id;
    private final DataBufferFactory bufferFactory = new DefaultDataBufferFactory();
    private final HandshakeInfo handshakeInfo;
    private final Map<String, Object> attributes = new HashMap<>();
    private final Sinks.Many<WebSocketMessage> inbound =
            Sinks.many().unicast().onBackpressureBuffer();
    private final List<String> sentFrames = new CopyOnWriteArrayList<>();
    private final AtomicBoolean open = new AtomicBoolean(true);
    private final AtomicBoolean sendCompleted = new AtomicBoolean(false);

    private volatile Disposable outboundSubscription;
    private volatile CloseStatus closeStatus;

    /**
     * Creates a session.
     *
     * @param id the session identifier the broker will see
     */
    public FakeWebSocketSession(String id) {
        this.id = id;
        this.handshakeInfo = new HandshakeInfo(
                URI.create("ws://localhost:8080/ws/stomp"),
                new HttpHeaders(),
                Mono.empty(),
                "v12.stomp");
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public HandshakeInfo getHandshakeInfo() {
        return handshakeInfo;
    }

    @Override
    public DataBufferFactory bufferFactory() {
        return bufferFactory;
    }

    @Override
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    @Override
    public Flux<WebSocketMessage> receive() {
        return inbound.asFlux();
    }

    @Override
    public Mono<Void> send(Publisher<WebSocketMessage> messages) {
        return Mono.create(sink -> outboundSubscription = Flux.from(messages)
                .doOnNext(message -> sentFrames.add(message.getPayloadAsText(StandardCharsets.UTF_8)))
                .doOnComplete(() -> {
                    sendCompleted.set(true);
                    // Reactor Netty closes the socket when the outbound publisher completes, which
                    // in turn terminates the inbound stream. Modelling that here is what makes the
                    // broker's session-teardown path observable in a unit test.
                    open.set(false);
                    inbound.tryEmitComplete();
                    sink.success();
                })
                .doOnError(sink::error)
                .subscribe());
    }

    @Override
    public WebSocketMessage textMessage(String payload) {
        return new WebSocketMessage(WebSocketMessage.Type.TEXT,
                bufferFactory.wrap(payload.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public WebSocketMessage binaryMessage(Function<DataBufferFactory, DataBuffer> payloadFactory) {
        return new WebSocketMessage(WebSocketMessage.Type.BINARY, payloadFactory.apply(bufferFactory));
    }

    @Override
    public WebSocketMessage pingMessage(Function<DataBufferFactory, DataBuffer> payloadFactory) {
        return new WebSocketMessage(WebSocketMessage.Type.PING, payloadFactory.apply(bufferFactory));
    }

    @Override
    public WebSocketMessage pongMessage(Function<DataBufferFactory, DataBuffer> payloadFactory) {
        return new WebSocketMessage(WebSocketMessage.Type.PONG, payloadFactory.apply(bufferFactory));
    }

    @Override
    public boolean isOpen() {
        return open.get();
    }

    @Override
    public Mono<CloseStatus> closeStatus() {
        return closeStatus == null ? Mono.empty() : Mono.just(closeStatus);
    }

    @Override
    public Mono<Void> close(CloseStatus status) {
        this.closeStatus = status;
        open.set(false);
        inbound.tryEmitComplete();
        return Mono.empty();
    }

    /**
     * Pushes a raw payload into the session as if the client had sent a text frame.
     *
     * @param payload the frame text, including its NULL terminator where applicable
     */
    public void clientSends(String payload) {
        inbound.tryEmitNext(textMessage(payload));
    }

    /**
     * Pushes a raw payload as a binary frame, to exercise the broker's handling of clients that
     * send STOMP over binary rather than text frames.
     *
     * @param payload the frame text
     */
    public void clientSendsBinary(String payload) {
        byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
        inbound.tryEmitNext(new WebSocketMessage(
                WebSocketMessage.Type.BINARY, bufferFactory.wrap(bytes)));
    }

    /**
     * Pushes a frame type the broker is expected to ignore.
     */
    public void clientSendsPing() {
        inbound.tryEmitNext(new WebSocketMessage(
                WebSocketMessage.Type.PING, bufferFactory.wrap(new byte[0])));
    }

    /**
     * Completes the inbound stream, as a clean client disconnect would.
     */
    public void clientDisconnects() {
        inbound.tryEmitComplete();
    }

    /**
     * @return every frame the broker has written, in order
     */
    public List<String> sentFrames() {
        return Collections.unmodifiableList(new ArrayList<>(sentFrames));
    }

    /**
     * @return the concatenation of every frame written, for substring assertions
     */
    public String allSent() {
        return String.join("", sentFrames);
    }

    /**
     * @return the first frame whose command matches, or {@code null} when none did
     */
    public String firstFrame(String command) {
        return sentFrames.stream()
                .filter(frame -> frame.startsWith(command + "\n"))
                .findFirst()
                .orElse(null);
    }

    /**
     * @param command the STOMP command to count
     * @return how many frames of that command were written
     */
    public long countFrames(String command) {
        return sentFrames.stream().filter(frame -> frame.startsWith(command + "\n")).count();
    }

    /**
     * @return {@code true} once the broker has completed its outbound stream
     */
    public boolean outboundCompleted() {
        return sendCompleted.get();
    }

    /**
     * Disposes the outbound subscription, used to tidy up after a test.
     */
    public void dispose() {
        Disposable subscription = outboundSubscription;
        if (subscription != null) {
            subscription.dispose();
        }
    }
}
