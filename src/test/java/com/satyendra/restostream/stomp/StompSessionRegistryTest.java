/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.stomp;

import com.satyendra.restostream.broadcast.BroadcastKind;
import com.satyendra.restostream.support.FakeWebSocketSession;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Unit suite for the broker's routing table and per-session bookkeeping.</p>
 *
 * <p>Two behaviours here are load bearing for a live restaurant and are asserted explicitly: a
 * broadcast must reach every matching subscription on every session, and a dashboard that cannot
 * keep up must be disconnected rather than allowed to slow the broadcast down or pin broker memory.
 * A table turning red cannot be made to wait for one tablet on bad Wi-Fi.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@DisplayName("StompSessionRegistry")
class StompSessionRegistryTest {

    private static final byte[] BODY = "{\"tableId\":\"T-01\"}".getBytes(StandardCharsets.UTF_8);

    private StompSessionRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new StompSessionRegistry(new SimpleMeterRegistry());
    }

    private StompClientSession connected(FakeWebSocketSession transport) {
        StompClientSession session = new StompClientSession(transport, 32);
        session.markConnected("1.2", Duration.ofSeconds(10), Duration.ofSeconds(10));
        registry.register(session);
        transport.send(session.outbound()).subscribe();
        return session;
    }

    // ------------------------------------------------------------------ //

    @Nested
    @DisplayName("destination matching")
    class DestinationMatching {

        @ParameterizedTest(name = "\"{0}\" matches \"{1}\": {2}")
        @CsvSource({
                "/topic/tables,/topic/tables,true",
                "/topic/tables,/topic/tables/T-01,false",
                "/topic/tables/**,/topic/tables/T-01,true",
                "/topic/tables/**,/topic/tables,true",
                "/topic/tables/*,/topic/tables/T-01,true",
                "/topic/tables/*,/topic/tables/T-01/extra,false",
                "/topic/tables/T-01,/topic/tables/T-01,true",
                "/topic/tables/T-01,/topic/tables/T-02,false",
                "/topic/sla-alerts,/topic/sla-alerts,true"
        })
        @DisplayName("supports exact destinations and Ant-style patterns")
        void matchesExactAndPatterns(String subscribed, String published, boolean expected) {
            assertThat(StompClientSession.matches(subscribed, published)).isEqualTo(expected);
        }

        @Test
        @DisplayName("** matches zero segments, which is why fan-out must de-duplicate")
        void doubleWildcardMatchesTheParentDestination() {
            assertThat(StompClientSession.matches("/topic/tables/**", "/topic/tables"))
                    .as("Ant's ** spans zero or more segments, so this subscription matches both "
                            + "destinations a table state change is addressed to")
                    .isTrue();
            assertThat(StompClientSession.matches("/topic/tables/*", "/topic/tables"))
                    .as("a single * requires exactly one further segment")
                    .isFalse();
        }

        @Test
        @DisplayName("recognises a pattern destination")
        void recognisesPatterns() {
            assertThat(StompClientSession.isPattern("/topic/tables/**")).isTrue();
            assertThat(StompClientSession.isPattern("/topic/tables/*")).isTrue();
            assertThat(StompClientSession.isPattern("/topic/tables")).isFalse();
            assertThatNullPointerException()
                    .isThrownBy(() -> StompClientSession.isPattern(null));
        }
    }

    @Nested
    @DisplayName("fan-out")
    class FanOut {

        @Test
        @DisplayName("delivers one broadcast to every matching subscription on every session")
        void deliversToEverySession() {
            FakeWebSocketSession firstTransport = new FakeWebSocketSession("s1");
            FakeWebSocketSession secondTransport = new FakeWebSocketSession("s2");
            StompClientSession first = connected(firstTransport);
            StompClientSession second = connected(secondTransport);
            first.addSubscription("a", "/topic/tables");
            first.addSubscription("b", "/topic/tables/T-01");
            second.addSubscription("c", "/topic/tables");

            int delivered = registry.broadcast("/topic/tables", BroadcastKind.TABLE_STATE, BODY);

            assertThat(delivered).isEqualTo(2);
            assertThat(firstTransport.countFrames("MESSAGE")).isEqualTo(1);
            assertThat(secondTransport.countFrames("MESSAGE")).isEqualTo(1);
            assertThat(firstTransport.firstFrame("MESSAGE")).contains("subscription:a");
            firstTransport.dispose();
            secondTransport.dispose();
        }

        @Test
        @DisplayName("sends one frame per subscription even when it matches several destinations")
        void deduplicatesAcrossDestinationsOfOneEnvelope() {
            FakeWebSocketSession transport = new FakeWebSocketSession("s1");
            StompClientSession session = connected(transport);
            session.addSubscription("wild", "/topic/tables/**");

            // Exactly how a table state change is addressed: aggregate feed plus the table's own.
            int delivered = registry.broadcastAll(
                    List.of("/topic/tables", "/topic/tables/T-01"),
                    BroadcastKind.TABLE_STATE, BODY);

            assertThat(delivered)
                    .as("a wildcard subscription matches both destinations but must be sent once")
                    .isEqualTo(1);
            assertThat(transport.countFrames("MESSAGE")).isEqualTo(1);
            assertThat(transport.firstFrame("MESSAGE"))
                    .as("delivery uses the first matching destination, which is the canonical feed")
                    .contains("destination:/topic/tables\n");
            transport.dispose();
        }

        @Test
        @DisplayName("gives an exact per-table subscription its own destination header")
        void exactSubscriptionKeepsItsOwnDestination() {
            FakeWebSocketSession transport = new FakeWebSocketSession("s1");
            StompClientSession session = connected(transport);
            session.addSubscription("tile", "/topic/tables/T-01");

            int delivered = registry.broadcastAll(
                    List.of("/topic/tables", "/topic/tables/T-01"),
                    BroadcastKind.TABLE_STATE, BODY);

            assertThat(delivered).isEqualTo(1);
            assertThat(transport.firstFrame("MESSAGE"))
                    .contains("destination:/topic/tables/T-01");
            transport.dispose();
        }

        @Test
        @DisplayName("rejects a null destination list")
        void rejectsNullDestinationList() {
            assertThatNullPointerException().isThrownBy(() ->
                    registry.broadcastAll(null, BroadcastKind.TABLE_STATE, BODY));
        }

        @Test
        @DisplayName("delivers a command result to the issuing session only")
        void deliversSessionScopedMessage() {
            FakeWebSocketSession firstTransport = new FakeWebSocketSession("s1");
            FakeWebSocketSession secondTransport = new FakeWebSocketSession("s2");
            connected(firstTransport).addSubscription("a", "/topic/command-results");
            connected(secondTransport).addSubscription("b", "/topic/command-results");

            int delivered = registry.sendTo(
                    "s1", "/topic/command-results", BroadcastKind.TABLE_STATE, BODY);

            assertThat(delivered).isEqualTo(1);
            assertThat(firstTransport.countFrames("MESSAGE")).isEqualTo(1);
            assertThat(secondTransport.countFrames("MESSAGE"))
                    .as("a command result belongs to the dashboard that issued it")
                    .isZero();
            firstTransport.dispose();
            secondTransport.dispose();
        }

        @Test
        @DisplayName("silently skips an unknown session and an unsubscribed destination")
        void skipsUnreachableTargets() {
            FakeWebSocketSession transport = new FakeWebSocketSession("s1");
            connected(transport);

            assertThat(registry.sendTo("nobody", "/topic/x", BroadcastKind.TABLE_STATE, BODY))
                    .isZero();
            assertThat(registry.sendTo("s1", "/topic/x", BroadcastKind.TABLE_STATE, BODY))
                    .as("the session exists but is not listening to that destination")
                    .isZero();
            transport.dispose();
        }

        @Test
        @DisplayName("never delivers to a session that has not completed the handshake")
        void skipsUnconnectedSessions() {
            FakeWebSocketSession transport = new FakeWebSocketSession("s1");
            StompClientSession session = new StompClientSession(transport, 32);
            registry.register(session);
            session.addSubscription("a", "/topic/tables");

            assertThat(registry.broadcast("/topic/tables", BroadcastKind.TABLE_STATE, BODY))
                    .isZero();
            transport.dispose();
        }

        @Test
        @DisplayName("stamps a unique message id on every frame")
        void stampsUniqueMessageIds() {
            FakeWebSocketSession transport = new FakeWebSocketSession("s1");
            connected(transport).addSubscription("a", "/topic/tables");

            registry.broadcast("/topic/tables", BroadcastKind.TABLE_STATE, BODY);
            registry.broadcast("/topic/tables", BroadcastKind.TABLE_STATE, BODY);

            assertThat(transport.sentFrames()).hasSize(2);
            assertThat(transport.sentFrames().get(0)).contains("message-id:s1-1");
            assertThat(transport.sentFrames().get(1)).contains("message-id:s1-2");
            transport.dispose();
        }

        @Test
        @DisplayName("defends its contracts")
        void defendsContracts() {
            assertThatNullPointerException()
                    .isThrownBy(() -> new StompSessionRegistry(null));
            assertThatNullPointerException().isThrownBy(() -> registry.register(null));
            assertThatNullPointerException().isThrownBy(() -> registry.unregister(null));
            assertThatNullPointerException().isThrownBy(() ->
                    registry.broadcast(null, BroadcastKind.TABLE_STATE, BODY));
            assertThatNullPointerException().isThrownBy(() ->
                    registry.broadcast("/topic/x", BroadcastKind.TABLE_STATE, null));
            assertThatNullPointerException().isThrownBy(() ->
                    registry.sendTo(null, "/topic/x", BroadcastKind.TABLE_STATE, BODY));
            assertThat(registry.find("absent")).isNull();
            registry.unregister("absent");
        }
    }

    @Nested
    @DisplayName("slow consumer defence")
    class SlowConsumer {

        @Test
        @DisplayName("disconnects a dashboard whose outbound buffer overflows")
        void disconnectsSaturatedDashboard() {
            FakeWebSocketSession transport = new FakeWebSocketSession("slow");
            // No subscriber is attached to the outbound sink, so nothing is ever drained.
            StompClientSession session = new StompClientSession(transport, 2);
            session.markConnected("1.2", Duration.ofSeconds(10), Duration.ofSeconds(10));
            registry.register(session);
            session.addSubscription("a", "/topic/tables");

            for (int i = 0; i < 10; i++) {
                registry.broadcast("/topic/tables", BroadcastKind.TABLE_STATE, BODY);
            }

            assertThat(session.isClosing())
                    .as("a dashboard that cannot keep up is disconnected, not tolerated")
                    .isTrue();
            assertThat(registry.broadcast("/topic/tables", BroadcastKind.TABLE_STATE, BODY))
                    .isZero();
        }
    }

    @Nested
    @DisplayName("session bookkeeping")
    class Bookkeeping {

        @Test
        @DisplayName("tracks the connected dashboard count across registration and removal")
        void tracksSessionCount() {
            FakeWebSocketSession transport = new FakeWebSocketSession("s1");
            StompClientSession session = connected(transport);

            assertThat(registry.sessionCount()).isEqualTo(1);
            assertThat(registry.find("s1")).isSameAs(session);
            assertThat(registry.sessions()).containsExactly(session);

            registry.unregister("s1");

            assertThat(registry.sessionCount()).isZero();
            assertThat(session.isClosing())
                    .as("deregistering must also tear the session down")
                    .isTrue();
            transport.dispose();
        }

        @Test
        @DisplayName("refuses a duplicate subscription id and forgets one on unsubscribe")
        void managesSubscriptions() {
            FakeWebSocketSession transport = new FakeWebSocketSession("s1");
            StompClientSession session = connected(transport);

            assertThat(session.addSubscription("a", "/topic/tables")).isTrue();
            assertThat(session.addSubscription("a", "/topic/timers")).isFalse();
            assertThat(session.subscriptions()).containsExactly(
                    java.util.Map.entry("a", "/topic/tables"));
            assertThat(session.subscriptionsMatching("/topic/tables")).containsExactly("a");
            assertThat(session.subscriptionsMatching("/topic/timers")).isEmpty();
            assertThat(session.removeSubscription("a")).isEqualTo("/topic/tables");
            assertThat(session.removeSubscription("a")).isNull();
            assertThat(session.subscriptions()).isEmpty();
            transport.dispose();
        }

        @Test
        @DisplayName("records the negotiated handshake exactly once")
        void recordsHandshakeOnce() {
            FakeWebSocketSession transport = new FakeWebSocketSession("s1");
            StompClientSession session = new StompClientSession(transport, 8);

            assertThat(session.isConnected()).isFalse();
            assertThat(session.markConnected("1.2",
                    Duration.ofSeconds(5), Duration.ofSeconds(7))).isTrue();
            assertThat(session.markConnected("1.1", Duration.ZERO, Duration.ZERO))
                    .as("a second handshake must be reported as a protocol error, not accepted")
                    .isFalse();
            assertThat(session.negotiatedVersion()).isEqualTo("1.1");
            assertThat(session.clientHeartbeatSend()).isEqualTo(Duration.ZERO);
            assertThat(session.clientHeartbeatReceive()).isEqualTo(Duration.ZERO);
            assertThat(session.sessionId()).isEqualTo("s1");
            assertThat(session.accumulator()).isNotNull();
            transport.dispose();
        }

        @Test
        @DisplayName("heart-beats only sessions that asked to receive them")
        void heartbeatsOnlyWillingSessions() {
            FakeWebSocketSession willingTransport = new FakeWebSocketSession("willing");
            FakeWebSocketSession unwillingTransport = new FakeWebSocketSession("unwilling");
            connected(willingTransport);
            StompClientSession unwilling = new StompClientSession(unwillingTransport, 8);
            unwilling.markConnected("1.2", Duration.ZERO, Duration.ZERO);
            registry.register(unwilling);
            unwillingTransport.send(unwilling.outbound()).subscribe();

            assertThat(registry.heartbeatAll()).isEqualTo(1);
            assertThat(willingTransport.allSent()).isEqualTo("\n");
            assertThat(unwillingTransport.allSent()).isEmpty();
            willingTransport.dispose();
            unwillingTransport.dispose();
        }

        @Test
        @DisplayName("stops accepting frames once it has begun closing")
        void refusesFramesAfterClosing() {
            FakeWebSocketSession transport = new FakeWebSocketSession("s1");
            StompClientSession session = connected(transport);

            session.closeQuietly();

            assertThat(session.isClosing()).isTrue();
            assertThat(session.send(StompFrameWriter.receipt("r"))).isFalse();
            assertThat(session.sendHeartbeat()).isFalse();
            session.closeQuietly();
            transport.dispose();
        }

        @Test
        @DisplayName("defends its construction contract")
        void defendsContracts() {
            FakeWebSocketSession transport = new FakeWebSocketSession("s1");
            assertThatNullPointerException()
                    .isThrownBy(() -> new StompClientSession(null, 8));
            assertThatExceptionOfType(IllegalArgumentException.class)
                    .isThrownBy(() -> new StompClientSession(transport, 0))
                    .withMessageContaining("must be positive");
            StompClientSession session = new StompClientSession(transport, 8);
            assertThatNullPointerException().isThrownBy(() -> session.send(null));
            assertThatNullPointerException()
                    .isThrownBy(() -> session.addSubscription(null, "/topic/x"));
            assertThatNullPointerException()
                    .isThrownBy(() -> session.addSubscription("a", null));
            assertThatNullPointerException().isThrownBy(() -> session.removeSubscription(null));
            assertThatNullPointerException().isThrownBy(() -> session.subscriptionsMatching(null));
            assertThatNullPointerException()
                    .isThrownBy(() -> session.markConnected(null, Duration.ZERO, Duration.ZERO));
            transport.dispose();
        }
    }
}
