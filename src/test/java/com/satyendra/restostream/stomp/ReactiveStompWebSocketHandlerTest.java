/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.stomp;

import com.satyendra.restostream.broadcast.BroadcastKind;
import com.satyendra.restostream.config.RestoStreamProperties;
import com.satyendra.restostream.support.FakeWebSocketSession;
import com.satyendra.restostream.support.TestProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Unit suite for the reactive STOMP broker, driven end to end over an in-memory
 * {@link FakeWebSocketSession}: real frames in, real frames out, no network and no Redis.</p>
 *
 * <p>The assertions deliberately cover the protocol's unhappy paths as thoroughly as its happy one.
 * A broker that only works for a well-behaved client is a liability — a frame before
 * {@code CONNECT}, a duplicate subscription id, a subscription to an application destination, or a
 * client-only frame arriving from the wrong direction must each produce a defined response rather
 * than an unhandled exception on an event-loop thread.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@DisplayName("ReactiveStompWebSocketHandler")
class ReactiveStompWebSocketHandlerTest {

    private static final char NULL = '\u0000';

    private StompSessionRegistry registry;
    private RecordingGateway gateway;
    private ReactiveStompWebSocketHandler handler;
    private FakeWebSocketSession session;
    private Disposable connection;

    @BeforeEach
    void setUp() {
        registry = new StompSessionRegistry(new SimpleMeterRegistry());
        gateway = new RecordingGateway();
        handler = new ReactiveStompWebSocketHandler(registry, gateway, TestProperties.defaults());
        session = new FakeWebSocketSession("sess-1");
    }

    @AfterEach
    void tearDown() {
        if (connection != null) {
            connection.dispose();
        }
        session.dispose();
    }

    private void open() {
        connection = handler.handle(session).subscribe();
    }

    private void connect() {
        open();
        session.clientSends("CONNECT\naccept-version:1.2\nheart-beat:10000,10000\n\n" + NULL);
    }

    private void subscribe(String id, String destination) {
        session.clientSends("SUBSCRIBE\nid:" + id + "\ndestination:" + destination
                + "\nreceipt:r-" + id + "\n\n" + NULL);
    }

    // ------------------------------------------------------------------ //

    @Test
    @DisplayName("advertises the STOMP sub-protocols during the WebSocket handshake")
    void advertisesSubProtocols() {
        assertThat(handler.getSubProtocols())
                .containsExactly("v12.stomp", "v11.stomp", "v10.stomp");
    }

    @Nested
    @DisplayName("handshake")
    class Handshake {

        @Test
        @DisplayName("answers CONNECT with CONNECTED carrying version, session and heart-beat")
        void completesHandshake() {
            connect();

            String connected = session.firstFrame("CONNECTED");
            assertThat(connected).isNotNull();
            assertThat(connected)
                    .contains("version:1.2")
                    .contains("session:sess-1")
                    .contains("server:RestoStream/1.0.0")
                    .contains("heart-beat:10000,10000");
            assertThat(registry.sessionCount()).isEqualTo(1);
            assertThat(registry.find("sess-1").isConnected()).isTrue();
        }

        @Test
        @DisplayName("accepts a STOMP frame as an alias for CONNECT")
        void acceptsStompAlias() {
            open();
            session.clientSends("STOMP\naccept-version:1.2\n\n" + NULL);

            assertThat(session.firstFrame("CONNECTED")).contains("version:1.2");
        }

        @Test
        @DisplayName("accepts STOMP over binary frames as well as text frames")
        void acceptsBinaryFrames() {
            open();
            session.clientSendsBinary("CONNECT\naccept-version:1.2\n\n" + NULL);

            assertThat(session.firstFrame("CONNECTED")).isNotNull();
        }

        @Test
        @DisplayName("ignores PING frames, which the container handles")
        void ignoresPing() {
            open();
            session.clientSendsPing();

            assertThat(session.sentFrames()).isEmpty();
        }

        @Test
        @DisplayName("treats a version-less CONNECT as STOMP 1.0")
        void defaultsToVersionOnePointZero() {
            open();
            session.clientSends("CONNECT\n\n" + NULL);

            assertThat(session.firstFrame("CONNECTED")).contains("version:1.0");
        }

        @Test
        @DisplayName("rejects a client that offers no supported version")
        void rejectsUnsupportedVersion() {
            open();
            session.clientSends("CONNECT\naccept-version:2.0,3.0\n\n" + NULL);

            assertThat(session.firstFrame("ERROR"))
                    .contains("message:unsupported protocol version")
                    .contains("2.0,3.0");
            assertThat(session.firstFrame("CONNECTED")).isNull();
        }

        @Test
        @DisplayName("rejects a second CONNECT on an established session")
        void rejectsDuplicateConnect() {
            connect();
            session.clientSends("CONNECT\naccept-version:1.2\n\n" + NULL);

            assertThat(session.firstFrame("ERROR")).contains("message:already connected");
        }

        @Test
        @DisplayName("rejects any frame that arrives before CONNECT")
        void rejectsFrameBeforeConnect() {
            open();
            session.clientSends("SUBSCRIBE\nid:s0\ndestination:/topic/tables\n\n" + NULL);

            assertThat(session.firstFrame("ERROR"))
                    .contains("message:not connected")
                    .contains("SUBSCRIBE frame was received before CONNECT");
            assertThat(gateway.subscribes).isEmpty();
        }

        @Test
        @DisplayName("negotiates no server heart-beat when the client disables it")
        void negotiatesDisabledHeartbeat() {
            open();
            session.clientSends("CONNECT\naccept-version:1.2\nheart-beat:0,0\n\n" + NULL);

            assertThat(session.firstFrame("CONNECTED")).contains("heart-beat:0,0");
        }

        @Test
        @DisplayName("takes the slower cadence when the client asks for a faster one")
        void negotiatesSlowerCadence() {
            open();
            session.clientSends("CONNECT\naccept-version:1.2\nheart-beat:1000,1000\n\n" + NULL);

            assertThat(session.firstFrame("CONNECTED"))
                    .as("the server cannot promise faster than its configured 10s cadence")
                    .contains("heart-beat:10000,10000");
        }
    }

    @Nested
    @DisplayName("subscriptions")
    class Subscriptions {

        @Test
        @DisplayName("registers a subscription, receipts it and primes the application gateway")
        void registersSubscription() {
            connect();
            subscribe("sub-0", "/topic/tables");

            assertThat(session.firstFrame("RECEIPT")).contains("receipt-id:r-sub-0");
            assertThat(registry.find("sess-1").subscriptions())
                    .containsEntry("sub-0", "/topic/tables");
            assertThat(gateway.subscribes).containsExactly("sub-0@/topic/tables");
        }

        @Test
        @DisplayName("delivers a broadcast to every matching subscription")
        void deliversBroadcast() {
            connect();
            subscribe("sub-0", "/topic/tables");
            subscribe("sub-1", "/topic/tables/**");

            int delivered = registry.broadcast("/topic/tables/T-01", BroadcastKind.TABLE_STATE,
                    "{\"tableId\":\"T-01\"}".getBytes(StandardCharsets.UTF_8));

            assertThat(delivered)
                    .as("only the Ant pattern matches a per-table destination")
                    .isEqualTo(1);
            assertThat(session.firstFrame("MESSAGE"))
                    .contains("destination:/topic/tables/T-01")
                    .contains("subscription:sub-1")
                    .contains("content-kind:TABLE_STATE")
                    .contains("{\"tableId\":\"T-01\"}");
        }

        @Test
        @DisplayName("stops delivering after UNSUBSCRIBE")
        void stopsDeliveringAfterUnsubscribe() {
            connect();
            subscribe("sub-0", "/topic/tables");
            session.clientSends("UNSUBSCRIBE\nid:sub-0\nreceipt:r-u\n\n" + NULL);

            int delivered = registry.broadcast("/topic/tables", BroadcastKind.TABLE_STATE,
                    "{}".getBytes(StandardCharsets.UTF_8));

            assertThat(delivered).isZero();
            assertThat(registry.find("sess-1").subscriptions()).isEmpty();
            assertThat(session.firstFrame("RECEIPT")).isNotNull();
        }

        @Test
        @DisplayName("tolerates UNSUBSCRIBE for an unknown id")
        void toleratesUnknownUnsubscribe() {
            connect();
            session.clientSends("UNSUBSCRIBE\nid:never-existed\nreceipt:r-u\n\n" + NULL);

            assertThat(session.countFrames("ERROR")).isZero();
            assertThat(session.firstFrame("RECEIPT")).contains("receipt-id:r-u");
        }

        @ParameterizedTest(name = "a SUBSCRIBE missing {0} is refused")
        @CsvSource({"id,SUBSCRIBE\\ndestination:/topic/tables",
                "destination,SUBSCRIBE\\nid:sub-0"})
        @DisplayName("refuses a SUBSCRIBE missing a mandatory header")
        void refusesIncompleteSubscribe(String missing, String frame) {
            connect();
            session.clientSends(frame.replace("\\n", "\n") + "\n\n" + NULL);

            assertThat(session.firstFrame("ERROR")).contains("missing " + missing + " header");
        }

        @Test
        @DisplayName("refuses a duplicate subscription id, as the specification requires")
        void refusesDuplicateSubscriptionId() {
            connect();
            subscribe("sub-0", "/topic/tables");
            session.clientSends(
                    "SUBSCRIBE\nid:sub-0\ndestination:/topic/timers\n\n" + NULL);

            assertThat(session.firstFrame("ERROR")).contains("duplicate subscription id");
        }

        @ParameterizedTest(name = "refuses a subscription to {0}")
        @ValueSource(strings = {"/app/tables/signal", "/queue/private", "/tables"})
        @DisplayName("refuses a subscription outside the topic prefix")
        void refusesNonTopicSubscription(String destination) {
            connect();
            session.clientSends(
                    "SUBSCRIBE\nid:sub-0\ndestination:" + destination + "\n\n" + NULL);

            assertThat(session.firstFrame("ERROR"))
                    .contains("message:illegal destination")
                    .contains(destination);
        }
    }

    @Nested
    @DisplayName("commands")
    class Commands {

        @Test
        @DisplayName("hands a SEND on an application destination to the gateway and receipts it")
        void delegatesSend() {
            connect();
            String body = "{\"tableId\":\"T-01\",\"signal\":\"GUEST_SEATED\"}";
            session.clientSends("SEND\ndestination:/app/tables/signal\nreceipt:r-send\n\n"
                    + body + NULL);

            assertThat(session.firstFrame("RECEIPT")).contains("receipt-id:r-send");
            assertThat(gateway.sends).containsExactly("/app/tables/signal=" + body);
        }

        @Test
        @DisplayName("refuses a SEND outside the application prefix")
        void refusesSendToTopic() {
            connect();
            session.clientSends("SEND\ndestination:/topic/tables\n\n{}" + NULL);

            assertThat(session.firstFrame("ERROR"))
                    .contains("message:illegal destination")
                    .contains("commands are only accepted under '/app'");
            assertThat(gateway.sends).isEmpty();
        }

        @Test
        @DisplayName("refuses a SEND with no destination")
        void refusesSendWithoutDestination() {
            connect();
            session.clientSends("SEND\n\n{}" + NULL);

            assertThat(session.firstFrame("ERROR")).contains("missing destination header");
        }

        @Test
        @DisplayName("absorbs a gateway failure instead of tearing down the session")
        void absorbsGatewayFailure() {
            gateway.failSends = true;
            connect();
            subscribe("sub-0", "/topic/tables");
            session.clientSends("SEND\ndestination:/app/tables/signal\n\n{}" + NULL);

            assertThat(session.countFrames("ERROR"))
                    .as("an application failure is not a protocol violation")
                    .isZero();

            int delivered = registry.broadcast("/topic/tables", BroadcastKind.TABLE_STATE,
                    "{}".getBytes(StandardCharsets.UTF_8));
            assertThat(delivered)
                    .as("the session must still be live after an application failure")
                    .isEqualTo(1);
        }

        @Test
        @DisplayName("absorbs a priming failure on SUBSCRIBE")
        void absorbsPrimingFailure() {
            gateway.failSubscribes = true;
            connect();
            subscribe("sub-0", "/topic/tables");

            assertThat(session.countFrames("ERROR")).isZero();
            assertThat(registry.find("sess-1").subscriptions()).containsKey("sub-0");
        }

        @Test
        @DisplayName("accepts and receipts ACK and NACK without treating them as errors")
        void acceptsAckAndNack() {
            connect();
            session.clientSends("ACK\nid:m1\nreceipt:r-ack\n\n" + NULL);
            session.clientSends("NACK\nid:m2\nreceipt:r-nack\n\n" + NULL);

            assertThat(session.countFrames("RECEIPT")).isEqualTo(2);
            assertThat(session.countFrames("ERROR")).isZero();
        }

        @ParameterizedTest(name = "refuses {0}")
        @ValueSource(strings = {"BEGIN", "COMMIT", "ABORT"})
        @DisplayName("refuses transaction frames, which this broker does not implement")
        void refusesTransactions(String command) {
            connect();
            session.clientSends(command + "\ntransaction:tx-1\n\n" + NULL);

            assertThat(session.firstFrame("ERROR"))
                    .contains("message:transactions are not supported");
        }

        @Test
        @DisplayName("refuses a server-to-client frame sent by a client")
        void refusesServerFrameFromClient() {
            connect();
            session.clientSends("MESSAGE\ndestination:/topic/tables\nsubscription:x\n\n{}" + NULL);

            assertThat(session.firstFrame("ERROR"))
                    .contains("message:unexpected frame")
                    .contains("must not be sent by a client");
        }

        @Test
        @DisplayName("closes the session cleanly on DISCONNECT and deregisters it")
        void handlesDisconnect() {
            connect();
            session.clientSends("DISCONNECT\nreceipt:r-bye\n\n" + NULL);

            assertThat(session.firstFrame("RECEIPT")).contains("receipt-id:r-bye");
            assertThat(registry.sessionCount())
                    .as("completing the outbound sink ends the session and deregisters it")
                    .isZero();
        }

        @Test
        @DisplayName("deregisters a session whose client simply vanishes")
        void deregistersOnClientDisconnect() {
            connect();
            subscribe("sub-0", "/topic/tables");
            assertThat(registry.sessionCount()).isEqualTo(1);

            session.clientDisconnects();

            assertThat(registry.sessionCount()).isZero();
        }

        @Test
        @DisplayName("closes a session that sends an undecodable frame")
        void closesOnMalformedFrame() {
            connect();
            session.clientSends("GARBAGE_COMMAND\nfoo:bar\n\n" + NULL);

            assertThat(session.firstFrame("ERROR")).contains("message:malformed frame");
        }

        @Test
        @DisplayName("treats a client heart-beat as liveness, not as a frame")
        void acceptsClientHeartbeat() {
            connect();
            long framesBefore = session.sentFrames().size();

            session.clientSends("\n");

            assertThat(session.sentFrames()).hasSize((int) framesBefore);
            assertThat(session.countFrames("ERROR")).isZero();
        }
    }

    @Nested
    @DisplayName("heart-beat negotiation")
    class HeartbeatNegotiation {

        @ParameterizedTest(name = "accept-version \"{0}\" negotiates {1}")
        @CsvSource({
                "1.2,1.2",
                "'1.0,1.1,1.2',1.2",
                "'1.1, 1.0',1.1",
                "1.0,1.0"
        })
        @DisplayName("chooses the highest mutually supported version")
        void negotiatesHighestVersion(String offered, String expected) {
            assertThat(ReactiveStompWebSocketHandler.negotiateVersion(offered)).isEqualTo(expected);
        }

        @Test
        @DisplayName("defaults an absent accept-version to 1.0 and refuses an unknown one")
        void versionEdgeCases() {
            assertThat(ReactiveStompWebSocketHandler.negotiateVersion(null)).isEqualTo("1.0");
            assertThat(ReactiveStompWebSocketHandler.negotiateVersion("  ")).isEqualTo("1.0");
            assertThat(ReactiveStompWebSocketHandler.negotiateVersion("9.9")).isNull();
        }

        @Test
        @DisplayName("parses a heart-beat header into send and receive intervals")
        void parsesHeartbeatHeader() {
            assertThat(ReactiveStompWebSocketHandler.parseHeartbeat("5000,20000"))
                    .containsExactly(Duration.ofSeconds(5), Duration.ofSeconds(20));
            assertThat(ReactiveStompWebSocketHandler.parseHeartbeat(" 100 , 200 "))
                    .containsExactly(Duration.ofMillis(100), Duration.ofMillis(200));
        }

        @ParameterizedTest(name = "a heart-beat header of \"{0}\" disables heart-beating")
        @ValueSource(strings = {"", "   ", "nonsense", "1000", "1000,2000,3000", "a,b", "-1,-1"})
        @DisplayName("a malformed heart-beat header disables heart-beating rather than failing")
        void malformedHeartbeatIsIgnored(String header) {
            assertThat(ReactiveStompWebSocketHandler.parseHeartbeat(header))
                    .allSatisfy(interval -> assertThat(interval).isLessThanOrEqualTo(Duration.ZERO));
        }

        @Test
        @DisplayName("a null heart-beat header disables heart-beating")
        void nullHeartbeatIsIgnored() {
            assertThat(ReactiveStompWebSocketHandler.parseHeartbeat(null))
                    .containsExactly(Duration.ZERO, Duration.ZERO);
        }

        @Test
        @DisplayName("applies the STOMP rule that zero on either side disables the direction")
        void effectiveIntervalRules() {
            Duration ten = Duration.ofSeconds(10);
            Duration thirty = Duration.ofSeconds(30);

            assertThat(ReactiveStompWebSocketHandler.effective(ten, thirty)).isEqualTo(thirty);
            assertThat(ReactiveStompWebSocketHandler.effective(thirty, ten)).isEqualTo(thirty);
            assertThat(ReactiveStompWebSocketHandler.effective(ten, Duration.ZERO))
                    .isEqualTo(Duration.ZERO);
            assertThat(ReactiveStompWebSocketHandler.effective(Duration.ZERO, ten))
                    .isEqualTo(Duration.ZERO);
            assertThat(ReactiveStompWebSocketHandler.effective(Duration.ofSeconds(-1), ten))
                    .isEqualTo(Duration.ZERO);
            assertThat(ReactiveStompWebSocketHandler.effective(ten, Duration.ofSeconds(-1)))
                    .isEqualTo(Duration.ZERO);
        }

        @Test
        @DisplayName("sends no heart-beats when the broker's cadence is configured to zero")
        void disabledServerCadenceSendsNothing() throws InterruptedException {
            RestoStreamProperties noHeartbeat = TestProperties.withStomp(
                    new RestoStreamProperties.Stomp("/ws/stomp", "/app", "/topic", 256,
                            Duration.ZERO, Duration.ZERO, Duration.ofMinutes(5)));
            ReactiveStompWebSocketHandler quietHandler =
                    new ReactiveStompWebSocketHandler(registry, gateway, noHeartbeat);
            FakeWebSocketSession quietSession = new FakeWebSocketSession("sess-quiet");

            Disposable quiet = quietHandler.handle(quietSession).subscribe();
            quietSession.clientSends("CONNECT\naccept-version:1.2\nheart-beat:50,50\n\n" + NULL);
            Thread.sleep(250L);

            assertThat(quietSession.firstFrame("CONNECTED")).contains("heart-beat:0,0");
            assertThat(quietSession.sentFrames())
                    .as("only the CONNECTED frame; no bare end-of-line heart-beats")
                    .hasSize(1);
            quiet.dispose();
            quietSession.dispose();
        }
    }

    /**
     * A recording stub of the application boundary, so that broker tests assert on protocol
     * behaviour without dragging Redis or the state engine into scope.
     */
    private static final class RecordingGateway implements StompApplicationGateway {

        private final List<String> subscribes = new ArrayList<>();
        private final List<String> sends = new ArrayList<>();
        private boolean failSubscribes;
        private boolean failSends;

        @Override
        public Mono<Void> onSubscribed(StompClientSession session,
                                       String subscriptionId,
                                       String destination) {
            subscribes.add(subscriptionId + '@' + destination);
            return failSubscribes
                    ? Mono.error(new IllegalStateException("state store unavailable"))
                    : Mono.empty();
        }

        @Override
        public Mono<Void> onSend(StompClientSession session, String destination, byte[] body) {
            sends.add(destination + '=' + new String(body, StandardCharsets.UTF_8));
            return failSends
                    ? Mono.error(new IllegalStateException("state store unavailable"))
                    : Mono.empty();
        }
    }
}
