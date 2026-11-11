/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.satyendra.restostream.domain.TableSnapshot;
import com.satyendra.restostream.stomp.CommandResult;
import com.satyendra.restostream.support.RedisIntegrationTestBase;
import com.satyendra.restostream.support.StompTestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>End-to-end integration suite for the live broadcaster: a real WebSocket, a real STOMP
 * handshake, real frames, and a real Redis behind the state engine.</p>
 *
 * <p>This is the suite that proves the central claim of the system — that a state change reaches
 * every connected dashboard instantly, whatever caused it. Both causes are exercised: an HTTP
 * command raised by an operator, and a STOMP command raised by a dashboard tap.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DisplayName("STOMP live broadcaster (integration)")
class StompBroadcastIntegrationTest extends RedisIntegrationTestBase {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    @LocalServerPort
    private int port;

    @Autowired
    private WebTestClient web;

    @Autowired
    private ObjectMapper objectMapper;

    private StompTestClient dashboard;

    @BeforeEach
    void connectDashboard() {
        dashboard = StompTestClient.connect("ws://localhost:" + port + "/ws/stomp");
    }

    @AfterEach
    void disconnectDashboard() {
        if (dashboard != null) {
            dashboard.close();
        }
    }

    @Test
    @DisplayName("completes the STOMP handshake and reports the negotiated session")
    void completesHandshake() {
        StompTestClient.Frame connected =
                dashboard.awaitFrame(frame -> "CONNECTED".equals(frame.command()), TIMEOUT);

        assertThat(connected.headers().get("version")).isEqualTo("1.2");
        assertThat(connected.headers().get("server")).startsWith("RestoStream/");
        assertThat(connected.headers().get("session")).isNotBlank();
        assertThat(connected.headers().get("heart-beat")).isEqualTo("10000,10000");
    }

    @Test
    @DisplayName("primes a new subscriber with the current floor plan on SUBSCRIBE")
    void primesSubscriberWithFloorPlan() {
        dashboard.subscribe("/topic/tables");

        StompTestClient.Frame snapshot = dashboard.awaitMessage("/topic/tables", "FLOOR_SNAPSHOT");
        List<?> floor = snapshot.bodyAs(objectMapper, List.class);

        assertThat(floor)
                .as("a dashboard opened mid-service must see the floor immediately, not a blank grid")
                .hasSizeGreaterThanOrEqualTo(12);
        assertThat(snapshot.body()).contains("\"tableId\":\"T-01\"").contains("\"colourName\":\"GREEN\"");
    }

    @Test
    @DisplayName("primes a per-table subscriber with that table only")
    void primesSubscriberWithOneTable() {
        dashboard.subscribe("/topic/tables/T-05");

        StompTestClient.Frame frame = dashboard.awaitMessage("/topic/tables/T-05", "TABLE_STATE");
        TableSnapshot table = frame.bodyAs(objectMapper, TableSnapshot.class);

        assertThat(table.tableId()).isEqualTo("T-05");
        assertThat(table.state().name()).isEqualTo("FREE");
    }

    @Test
    @DisplayName("broadcasts a state change caused by an HTTP command to a STOMP subscriber")
    void broadcastsHttpDrivenChange() {
        dashboard.subscribe("/topic/tables");
        dashboard.awaitMessage("/topic/tables", "FLOOR_SNAPSHOT");

        web.post().uri("/api/v1/tables/T-01/seat?occupancy=2")
                .exchange()
                .expectStatus().isOk()
                .expectBody(TableSnapshot.class);

        StompTestClient.Frame frame = dashboard.awaitMessage("/topic/tables",
                body -> body.contains("\"tableId\":\"T-01\"") && body.contains("\"state\":\"SEATED\""),
                TIMEOUT);
        TableSnapshot table = frame.bodyAs(objectMapper, TableSnapshot.class);

        assertThat(frame.headers().get("content-kind")).isEqualTo("TABLE_STATE");
        assertThat(frame.headers().get("subscription")).isNotBlank();
        assertThat(frame.headers().get("message-id")).isNotBlank();
        assertThat(table.state().name()).isEqualTo("SEATED");
        assertThat(table.colourHex()).isEqualTo("#C62828");
        assertThat(table.slaArmed()).isTrue();
        assertThat(table.occupancy()).isEqualTo(2);
    }

    @Test
    @DisplayName("delivers the same change on the per-table destination as well")
    void broadcastsToPerTableDestination() {
        dashboard.subscribe("/topic/tables/T-02");
        dashboard.awaitMessage("/topic/tables/T-02", "TABLE_STATE");

        web.post().uri("/api/v1/tables/T-02/seat").exchange().expectStatus().isOk().expectBody(TableSnapshot.class);

        StompTestClient.Frame frame = dashboard.awaitMessage("/topic/tables/T-02",
                body -> body.contains("\"state\":\"SEATED\""), TIMEOUT);

        assertThat(frame.bodyAs(objectMapper, TableSnapshot.class).tableId()).isEqualTo("T-02");
    }

    @Test
    @DisplayName("an Ant-pattern subscription receives every table exactly once")
    void matchesPatternSubscription() {
        dashboard.subscribe("/topic/tables/**");

        web.post().uri("/api/v1/tables/T-09/seat").exchange().expectStatus().isOk().expectBody(TableSnapshot.class);

        // `**` spans zero or more segments, so this subscription matches both destinations the
        // change is addressed to. The broker de-duplicates and delivers on the first match, which
        // is the canonical aggregate feed — one frame, not two.
        StompTestClient.Frame frame = dashboard.awaitMessage("/topic/tables",
                body -> body.contains("\"tableId\":\"T-09\"")
                        && body.contains("\"state\":\"SEATED\""), TIMEOUT);

        assertThat(frame.headers().get("destination")).isEqualTo("/topic/tables");

        long frames = dashboard.frames()
                .filter(f -> "MESSAGE".equals(f.command())
                        && "TABLE_STATE".equals(f.headers().get("content-kind"))
                        && f.body().contains("\"tableId\":\"T-09\""))
                .take(Duration.ofSeconds(2))
                .count()
                .block(TIMEOUT);

        assertThat(frames)
                .as("a wildcard subscriber must not be told about the same change twice")
                .isEqualTo(1L);
    }

    @Test
    @DisplayName("applies a dashboard tap sent over STOMP and answers on the command feed")
    void appliesStompCommand() {
        dashboard.subscribe("/topic/command-results");
        dashboard.subscribe("/topic/tables");
        dashboard.awaitMessage("/topic/tables", "FLOOR_SNAPSHOT");

        dashboard.send("/app/tables/signal", Map.of(
                "tableId", "T-03",
                "signal", "GUEST_SEATED",
                "occupancy", 4,
                "actor", "dashboard-ui",
                "correlationId", "corr-1"));

        StompTestClient.Frame resultFrame = dashboard.awaitMessage("/topic/command-results",
                body -> body.contains("corr-1"), TIMEOUT);
        CommandResult result = resultFrame.bodyAs(objectMapper, CommandResult.class);

        assertThat(result.status()).isEqualTo(CommandResult.Status.APPLIED);
        assertThat(result.tableId()).isEqualTo("T-03");
        assertThat(result.correlationId()).isEqualTo("corr-1");
        assertThat(result.snapshot()).isNotNull();
        assertThat(result.snapshot().state().name()).isEqualTo("SEATED");
        assertThat(result.snapshot().occupancy()).isEqualTo(4);

        dashboard.awaitMessage("/topic/tables",
                body -> body.contains("\"tableId\":\"T-03\"") && body.contains("\"state\":\"SEATED\""),
                TIMEOUT);

        web.get().uri("/api/v1/tables/T-03")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.state").isEqualTo("SEATED")
                .jsonPath("$.occupancy").isEqualTo(4);
    }

    @Test
    @DisplayName("reports a refused dashboard tap as REJECTED without closing the connection")
    void reportsRejectedStompCommand() {
        dashboard.subscribe("/topic/command-results");

        dashboard.send("/app/tables/signal", Map.of(
                "tableId", "T-04",
                "signal", "WAITER_ATTENDED",
                "correlationId", "corr-reject"));

        CommandResult result = dashboard.awaitMessage("/topic/command-results",
                        body -> body.contains("corr-reject"), TIMEOUT)
                .bodyAs(objectMapper, CommandResult.class);

        assertThat(result.status()).isEqualTo(CommandResult.Status.REJECTED);
        assertThat(result.message()).contains("only meaningful for");
        assertThat(result.snapshot()).isNull();

        // The connection must still be usable: a business rejection is not a protocol violation.
        dashboard.send("/app/tables/signal", Map.of(
                "tableId", "T-04", "signal", "GUEST_SEATED", "correlationId", "corr-after"));
        CommandResult after = dashboard.awaitMessage("/topic/command-results",
                        body -> body.contains("corr-after"), TIMEOUT)
                .bodyAs(objectMapper, CommandResult.class);
        assertThat(after.status()).isEqualTo(CommandResult.Status.APPLIED);
    }

    @Test
    @DisplayName("reports an unknown table on the command feed as FAILED")
    void reportsUnknownTable() {
        dashboard.subscribe("/topic/command-results");

        dashboard.send("/app/tables/signal", Map.of(
                "tableId", "T-999", "signal", "GUEST_SEATED", "correlationId", "corr-404"));

        CommandResult result = dashboard.awaitMessage("/topic/command-results",
                        body -> body.contains("corr-404"), TIMEOUT)
                .bodyAs(objectMapper, CommandResult.class);

        assertThat(result.status()).isEqualTo(CommandResult.Status.FAILED);
        assertThat(result.message()).contains("T-999");
    }

    @Test
    @DisplayName("re-sends the floor plan on demand")
    void refreshesFloorPlanOnDemand() {
        dashboard.subscribe("/topic/tables");
        dashboard.awaitMessage("/topic/tables", "FLOOR_SNAPSHOT");
        web.post().uri("/api/v1/tables/T-10/seat").exchange().expectStatus().isOk().expectBody(TableSnapshot.class);

        dashboard.write("SEND", Map.of("destination", "/app/floor/refresh"), "{}");

        StompTestClient.Frame refreshed = dashboard.awaitMessage("/topic/tables",
                body -> body.startsWith("[") && body.contains("\"tableId\":\"T-10\"")
                        && body.contains("\"state\":\"SEATED\""),
                TIMEOUT);

        assertThat(refreshed.headers().get("content-kind")).isEqualTo("FLOOR_SNAPSHOT");
    }

    @Test
    @DisplayName("broadcasts the live SLA countdown to the timer feed")
    void broadcastsCountdownTicks() {
        dashboard.subscribe("/topic/timers");

        web.post().uri("/api/v1/tables/T-11/seat").exchange().expectStatus().isOk().expectBody(TableSnapshot.class);

        StompTestClient.Frame tick = dashboard.awaitMessage("/topic/timers",
                body -> body.contains("\"tableId\":\"T-11\""), TIMEOUT);

        assertThat(tick.headers().get("content-kind")).isEqualTo("TIMER_TICK");
        assertThat(tick.body())
                .contains("\"slaKind\":\"WAITER_RESPONSE\"")
                .contains("\"remainingMillis\"")
                .contains("\"breached\":false");
    }

    @Test
    @DisplayName("stops delivering to a dashboard that unsubscribes")
    void stopsDeliveringAfterUnsubscribe() {
        String subscriptionId = dashboard.subscribe("/topic/tables");
        dashboard.awaitMessage("/topic/tables", "FLOOR_SNAPSHOT");

        dashboard.write("UNSUBSCRIBE",
                Map.of("id", subscriptionId, "receipt", "r-unsub"), null);
        dashboard.awaitFrame(frame -> "RECEIPT".equals(frame.command())
                && "r-unsub".equals(frame.headers().get("receipt-id")), TIMEOUT);

        web.post().uri("/api/v1/tables/T-12/seat").exchange().expectStatus().isOk().expectBody(TableSnapshot.class);

        // Counted by content-kind, not by substring: the FLOOR_SNAPSHOT delivered on subscribe
        // legitimately lists every table, T-12 included, so a bare substring match would always
        // find a hit and the assertion would prove nothing.
        long delivered = dashboard.frames()
                .filter(frame -> "MESSAGE".equals(frame.command())
                        && "TABLE_STATE".equals(frame.headers().get("content-kind"))
                        && frame.body().contains("\"tableId\":\"T-12\""))
                .take(Duration.ofSeconds(2))
                .count()
                .block(TIMEOUT);

        assertThat(delivered).isZero();
    }

    @Test
    @DisplayName("closes a connection that violates the protocol, with an ERROR frame first")
    void closesOnProtocolViolation() {
        try (StompTestClient hostile =
                     StompTestClient.connect("ws://localhost:" + port + "/ws/stomp")) {
            hostile.write("SUBSCRIBE",
                    Map.of("id", "s0", "destination", "/app/tables/signal"), null);

            StompTestClient.Frame error =
                    hostile.awaitFrame(frame -> "ERROR".equals(frame.command()), TIMEOUT);

            assertThat(error.headers().get("message")).isEqualTo("illegal destination");
            assertThat(error.body()).contains("/app/tables/signal");
        }
    }

    @Test
    @DisplayName("serves the same events over Server-Sent Events for clients with no STOMP library")
    void mirrorsBroadcastOverServerSentEvents() {
        // A plain WebClient rather than WebTestClient: an endless event stream has to be consumed
        // and then released, and WebClient's bodyToFlux decoder does that correctly on cancel,
        // whereas cancelling a streaming WebTestClient result abandons in-flight HTTP buffers and
        // trips Netty's leak detector. It also matches the approach taken for STOMP — verify a
        // public contract with an ordinary client, not with the server's own plumbing.
        org.springframework.web.reactive.function.client.WebClient client =
                org.springframework.web.reactive.function.client.WebClient.create(
                        "http://localhost:" + port);

        reactor.core.publisher.Flux<String> events = client.get()
                .uri("/api/v1/tables/stream")
                .accept(org.springframework.http.MediaType.TEXT_EVENT_STREAM)
                .retrieve()
                .bodyToFlux(String.class);

        String matched = events
                .doOnSubscribe(subscription -> seatOnAnotherThread("T-06"))
                .filter(payload -> payload.contains("\"tableId\":\"T-06\""))
                .next()
                .block(TIMEOUT);

        assertThat(matched)
                .as("the SSE mirror is fed by the same broadcaster as the STOMP broker")
                .isNotNull()
                .contains("\"state\":\"SEATED\"");
    }

    /**
     * Issues the state change from a separate thread, so the command is sent only once the Server-
     * Sent Events subscription is actually established and cannot be missed.
     *
     * @param tableId the table to seat
     */
    private void seatOnAnotherThread(String tableId) {
        reactor.core.publisher.Mono
                .fromRunnable(() -> web.post().uri("/api/v1/tables/" + tableId + "/seat")
                        .exchange().expectStatus().isOk())
                .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
                .subscribe();
    }
}
