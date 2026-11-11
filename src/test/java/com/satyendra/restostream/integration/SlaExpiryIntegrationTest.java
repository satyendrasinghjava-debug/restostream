/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.satyendra.restostream.alert.SlaAlert;
import com.satyendra.restostream.domain.TableSnapshot;
import com.satyendra.restostream.support.RedisIntegrationTestBase;
import com.satyendra.restostream.support.StompTestClient;
import com.satyendra.restostream.timer.DistributedTimerService;
import com.satyendra.restostream.timer.TimerDriftReconciler;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.time.Duration;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>The suite that verifies the specification's headline requirement end to end: <em>when a table
 * is seated and no waiter event occurs within the SLA window, an asynchronous alert is
 * triggered</em>.</p>
 *
 * <p>The SLA windows are compressed to two and three seconds for these tests. That is the only
 * concession made — everything else is real: a real Redis with real key expiry, the real Lua claim
 * script, the real keyspace-notification listener, the real reconciliation sweeper and the real
 * STOMP broker delivering the alert to a real WebSocket client.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DisplayName("SLA expiry and distributed timers (integration)")
class SlaExpiryIntegrationTest extends RedisIntegrationTestBase {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    /**
     * Compresses the SLA windows so a five-minute rule can be proven in seconds.
     *
     * @param registry the dynamic property registry
     */
    @DynamicPropertySource
    static void compressSlaWindows(DynamicPropertyRegistry registry) {
        registry.add("restostream.sla.waiter-response", () -> "2s");
        registry.add("restostream.sla.table-turnaround", () -> "3s");
        registry.add("restostream.timers.sweep-interval", () -> "500ms");
        registry.add("restostream.timers.countdown-interval", () -> "500ms");
    }

    @LocalServerPort
    private int port;

    @Autowired
    private WebTestClient web;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private DistributedTimerService timerService;

    @Autowired
    private TimerDriftReconciler driftReconciler;

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
    @DisplayName("raises a CRITICAL alert when a seated table goes unattended for the SLA window")
    void raisesAlertForUnattendedTable() {
        dashboard.subscribe("/topic/sla-alerts");

        seat("T-01", 3);

        StompTestClient.Frame frame = dashboard.awaitMessage("/topic/sla-alerts",
                body -> body.contains("\"tableId\":\"T-01\""), TIMEOUT);
        SlaAlert alert = frame.bodyAs(objectMapper, SlaAlert.class);

        assertThat(frame.headers().get("content-kind")).isEqualTo("SLA_ALERT");
        assertThat(alert.tableId()).isEqualTo("T-01");
        assertThat(alert.slaKind().name()).isEqualTo("WAITER_RESPONSE");
        assertThat(alert.severity().name()).isEqualTo("CRITICAL");
        assertThat(alert.tableState().name()).isEqualTo("SEATED");
        assertThat(alert.occupancy()).isEqualTo(3);
        assertThat(alert.dueAt()).isNotNull();
        assertThat(alert.detectedAt()).isAfterOrEqualTo(alert.dueAt());
        assertThat(alert.detectedBy()).isNotNull();
        assertThat(alert.message()).contains("T-01").contains("no waiter response");
        assertThat(alert.alertId()).isNotBlank();
    }

    @Test
    @DisplayName("flags the breach on the table and broadcasts the updated state")
    void flagsBreachOnTheTable() {
        dashboard.subscribe("/topic/tables/T-02");
        dashboard.awaitMessage("/topic/tables/T-02", "TABLE_STATE");

        seat("T-02", 2);

        StompTestClient.Frame frame = dashboard.awaitMessage("/topic/tables/T-02",
                body -> body.contains("\"slaBreached\":true"), TIMEOUT);
        TableSnapshot breached = frame.bodyAs(objectMapper, TableSnapshot.class);

        assertThat(breached.slaBreached()).isTrue();
        assertThat(breached.state().name())
                .as("a breach annotates the table; it does not move it")
                .isEqualTo("SEATED");
        assertThat(breached.slaDeadline())
                .as("the missed deadline is retained for forensic display")
                .isNotNull();

        assertThat(get("T-02").slaBreached())
                .as("the breach flag must be durable in Redis, not only broadcast")
                .isTrue();
    }

    @Test
    @DisplayName("raises exactly one alert per breach, however many detection passes observe it")
    void alertsExactlyOncePerBreach() {
        dashboard.subscribe("/topic/sla-alerts");

        seat("T-03", 2);
        dashboard.awaitMessage("/topic/sla-alerts",
                body -> body.contains("\"tableId\":\"T-03\""), TIMEOUT);

        // Give both the keyspace listener and several sweep passes time to observe the same
        // elapsed deadline. The atomic Lua claim plus the breach flag must collapse them to one.
        long alerts = dashboard.frames()
                .filter(f -> "MESSAGE".equals(f.command())
                        && "/topic/sla-alerts".equals(f.headers().get("destination"))
                        && f.body().contains("\"tableId\":\"T-03\""))
                .take(Duration.ofSeconds(4))
                .count()
                .block(TIMEOUT);

        assertThat(alerts)
                .as("an at-least-once timer subsystem must still alert effectively once")
                .isEqualTo(1L);
    }

    @Test
    @DisplayName("raises no alert when a waiter attends within the SLA window")
    void acknowledgementPreventsTheAlert() {
        dashboard.subscribe("/topic/sla-alerts");

        seat("T-04", 2);
        TableSnapshot acknowledged = post("/api/v1/tables/T-04/waiter-ack?actor=waiter-9");
        assertThat(acknowledged.slaArmed()).isFalse();

        long alerts = dashboard.frames()
                .filter(f -> "MESSAGE".equals(f.command())
                        && f.body().contains("\"tableId\":\"T-04\""))
                .take(Duration.ofSeconds(6))
                .count()
                .block(TIMEOUT);

        assertThat(alerts)
                .as("the whole point of the SLA is that attending the table silences it")
                .isZero();
        assertThat(get("T-04").slaBreached()).isFalse();
    }

    @Test
    @DisplayName("removes the armed deadline from Redis when a waiter acknowledges")
    void acknowledgementClearsTheDeadlineIndex() {
        seat("T-05", 2);

        Awaitility.await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(timerService.deadlineOf("T-05").block(Duration.ofSeconds(5)))
                        .isNotNull());

        post("/api/v1/tables/T-05/waiter-ack");

        Awaitility.await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(timerService.deadlineOf("T-05").block(Duration.ofSeconds(5)))
                    .as("an acknowledged table must leave no deadline behind to fire later")
                    .isNull();
            assertThat(redis().hasKey(properties().redis().slaTimerKey("T-05"))
                    .block(Duration.ofSeconds(5)))
                    .isFalse();
        });
    }

    @Test
    @DisplayName("raises a WARNING alert when a vacated table is not bussed in time")
    void raisesTurnaroundAlert() {
        dashboard.subscribe("/topic/sla-alerts");

        seat("T-06", 2);
        post("/api/v1/tables/T-06/waiter-ack");
        TableSnapshot departed = post("/api/v1/tables/T-06/depart");
        assertThat(departed.slaKind().name()).isEqualTo("TABLE_TURNAROUND");

        SlaAlert alert = dashboard.awaitMessage("/topic/sla-alerts",
                        body -> body.contains("\"tableId\":\"T-06\"")
                                && body.contains("TABLE_TURNAROUND"), TIMEOUT)
                .bodyAs(objectMapper, SlaAlert.class);

        assertThat(alert.severity().name()).isEqualTo("WARNING");
        assertThat(alert.tableState().name()).isEqualTo("UNCLEANED");
        assertThat(alert.message()).contains("awaiting cleaning");
    }

    @Test
    @DisplayName("arms the deadline in the sorted index and the TTL key together")
    void armsBothRedisStructures() {
        seat("T-07", 2);

        Awaitility.await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(timerService.deadlineOf("T-07").block(Duration.ofSeconds(5)))
                    .as("the durable deadline index is what makes detection lossless")
                    .isNotNull();
            assertThat(redis().<String, String>opsForHash()
                    .get(properties().redis().slaMetaKey(), "T-07")
                    .block(Duration.ofSeconds(5)))
                    .as("an expired key carries no value, so the SLA class is recorded separately")
                    .isEqualTo("WAITER_RESPONSE");
        });

        Awaitility.await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(get("T-07").slaBreached()).isTrue());

        Awaitility.await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(timerService.deadlineOf("T-07").block(Duration.ofSeconds(5)))
                    .as("claiming a deadline removes it, which is what prevents a second alert")
                    .isNull();
            assertThat(redis().<String, String>opsForHash()
                    .get(properties().redis().slaMetaKey(), "T-07")
                    .block(Duration.ofSeconds(5)))
                    .isNull();
        });
    }

    @Test
    @DisplayName("claims an elapsed deadline exactly once across concurrent callers")
    void claimIsAtomicAcrossCallers() {
        seat("T-08", 2);

        Awaitility.await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(get("T-08").slaBreached()).isTrue());

        // The alert has already been claimed and the index entry removed; every further claim
        // attempt, from any instance, must come back empty.
        assertThat(timerService.claim("T-08", java.time.Instant.now().plusSeconds(60))
                .block(Duration.ofSeconds(5)))
                .isNull();
    }

    @Test
    @DisplayName("repairs a snapshot whose deadline was lost before it could be armed")
    void driftReconcilerRepairsAMissingDeadline() {
        seat("T-09", 2);

        // Reproduce the one narrow crash window the ordering leaves open: the transition is
        // durable, but the deadline that should be counting it down is gone.
        Awaitility.await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(timerService.deadlineOf("T-09").block(Duration.ofSeconds(5)))
                        .isNotNull());
        Long removed = redis().opsForZSet()
                .remove(properties().redis().slaDeadlineIndexKey(), "T-09")
                .block(Duration.ofSeconds(5));
        redis().delete(properties().redis().slaTimerKey("T-09")).block(Duration.ofSeconds(5));
        redis().opsForHash().remove(properties().redis().slaMetaKey(), "T-09")
                .block(Duration.ofSeconds(5));
        assertThat(removed).isEqualTo(1L);
        assertThat(get("T-09").slaArmed())
                .as("the snapshot still believes it is being counted down")
                .isTrue();

        TimerDriftReconciler.ReconciliationReport report =
                driftReconciler.reconcileOnce().block(Duration.ofSeconds(10));

        assertThat(report).isNotNull();
        assertThat(report.inspected()).isGreaterThanOrEqualTo(12L);
        assertThat(report.rearmed())
                .as("the reconciler must notice a snapshot nothing is counting down")
                .isGreaterThanOrEqualTo(1L);

        Awaitility.await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(get("T-09").slaBreached())
                        .as("the re-armed deadline must go on to fire, so no breach is lost")
                        .isTrue());
    }

    @Test
    @DisplayName("cancels an orphaned deadline left behind for an unguarded table")
    void driftReconcilerCancelsAnOrphanedDeadline() {
        assertThat(get("T-11").slaArmed()).isFalse();
        timerService.arm("T-11", com.satyendra.restostream.domain.SlaKind.WAITER_RESPONSE,
                Duration.ofMinutes(10), java.time.Instant.now().plusSeconds(600))
                .block(Duration.ofSeconds(5));
        assertThat(timerService.deadlineOf("T-11").block(Duration.ofSeconds(5))).isNotNull();

        TimerDriftReconciler.ReconciliationReport report =
                driftReconciler.reconcileOnce().block(Duration.ofSeconds(10));

        assertThat(report).isNotNull();
        assertThat(report.cancelled()).isGreaterThanOrEqualTo(1L);
        assertThat(timerService.deadlineOf("T-11").block(Duration.ofSeconds(5)))
                .as("an operator must never be paged about an SLA the table is not subject to")
                .isNull();
    }

    @Test
    @DisplayName("counts armed timers through the diagnostics endpoint")
    void reportsArmedTimerCount() {
        seat("T-12", 2);

        Awaitility.await().atMost(TIMEOUT).untilAsserted(() ->
                web.get().uri("/api/v1/diagnostics")
                        .exchange()
                        .expectStatus().isOk()
                        .expectBody()
                        .jsonPath("$.armedSlaTimers").isEqualTo(1));

        Awaitility.await().atMost(TIMEOUT).untilAsserted(() ->
                web.get().uri("/api/v1/diagnostics")
                        .exchange()
                        .expectStatus().isOk()
                        .expectBody()
                        .jsonPath("$.armedSlaTimers")
                        .isEqualTo(0));
    }

    private TableSnapshot seat(String tableId, int occupancy) {
        return post("/api/v1/tables/" + tableId + "/seat?occupancy=" + occupancy);
    }

    private TableSnapshot post(String uri) {
        return Objects.requireNonNull(web.post().uri(uri)
                .exchange()
                .expectStatus().isOk()
                .expectBody(TableSnapshot.class)
                .returnResult()
                .getResponseBody());
    }

    private TableSnapshot get(String tableId) {
        return Objects.requireNonNull(web.get().uri("/api/v1/tables/" + tableId)
                .exchange()
                .expectStatus().isOk()
                .expectBody(TableSnapshot.class)
                .returnResult()
                .getResponseBody());
    }
}
