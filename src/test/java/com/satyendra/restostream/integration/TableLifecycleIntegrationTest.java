/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.integration;

import com.satyendra.restostream.domain.SignalType;
import com.satyendra.restostream.domain.TableSnapshot;
import com.satyendra.restostream.support.RedisIntegrationTestBase;
import com.satyendra.restostream.web.dto.SignalRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>End-to-end integration suite for the table lifecycle, driven through the real reactive HTTP
 * stack with {@link WebTestClient} against a real Redis container.</p>
 *
 * <p>These tests assert the behaviour that only an integration test can: that the Lua
 * compare-and-set actually increments the version in Redis, that provisioning is genuinely
 * idempotent via {@code HSETNX}, and that an illegal transition surfaces as {@code 409} through the
 * whole WebFlux error-handling chain rather than as a stack trace.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DisplayName("table lifecycle (integration)")
class TableLifecycleIntegrationTest extends RedisIntegrationTestBase {

    @Autowired
    private WebTestClient web;

    private WebTestClient client() {
        return web.mutate().responseTimeout(Duration.ofSeconds(20)).build();
    }

    @Test
    @DisplayName("provisions the configured floor plan into Redis at startup")
    void provisionsFloorPlan() {
        List<TableSnapshot> floor = client().get().uri("/api/v1/tables")
                .accept(MediaType.APPLICATION_JSON)
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(TableSnapshot.class)
                .returnResult()
                .getResponseBody();

        assertThat(floor).isNotNull().hasSizeGreaterThanOrEqualTo(12);
        assertThat(floor).allSatisfy(table -> {
            assertThat(table.state().name()).isEqualTo("FREE");
            assertThat(table.colourName()).isEqualTo("GREEN");
            assertThat(table.version()).isZero();
            assertThat(table.slaArmed()).isFalse();
        });
        assertThat(floor).extracting(TableSnapshot::tableId)
                .as("the floor plan is returned in a stable order so dashboards do not reshuffle")
                .isSorted();
    }

    @Test
    @DisplayName("walks a table through its whole lifecycle, incrementing the version each step")
    void walksTheLifecycle() {
        TableSnapshot seated = post("/api/v1/tables/T-01/seat?occupancy=3");
        assertThat(seated.state().name()).isEqualTo("SEATED");
        assertThat(seated.colourHex()).isEqualTo("#C62828");
        assertThat(seated.occupancy()).isEqualTo(3);
        assertThat(seated.version()).isEqualTo(1L);
        assertThat(seated.slaArmed()).isTrue();
        assertThat(seated.slaKind().name()).isEqualTo("WAITER_RESPONSE");

        TableSnapshot acknowledged = post("/api/v1/tables/T-01/waiter-ack?actor=waiter-7");
        assertThat(acknowledged.state().name()).isEqualTo("SEATED");
        assertThat(acknowledged.slaArmed())
                .as("acknowledging service disarms the SLA timer")
                .isFalse();
        assertThat(acknowledged.lastWaiterAckAt()).isNotNull();
        assertThat(acknowledged.version()).isEqualTo(2L);

        TableSnapshot departed = post("/api/v1/tables/T-01/depart");
        assertThat(departed.state().name()).isEqualTo("UNCLEANED");
        assertThat(departed.colourHex()).isEqualTo("#F9A825");
        assertThat(departed.slaKind().name()).isEqualTo("TABLE_TURNAROUND");
        assertThat(departed.version()).isEqualTo(3L);

        TableSnapshot free = post("/api/v1/tables/T-01/cleaned");
        assertThat(free.state().name()).isEqualTo("FREE");
        assertThat(free.slaArmed()).isFalse();
        assertThat(free.version()).isEqualTo(4L);

        TableSnapshot reread = get("/api/v1/tables/T-01");
        assertThat(reread.version())
                .as("the version in Redis must agree with what the command returned")
                .isEqualTo(4L);
        assertThat(reread.state().name()).isEqualTo("FREE");
    }

    @Test
    @DisplayName("refuses an illegal transition with 409 and a structured error body")
    void refusesIllegalTransition() {
        post("/api/v1/tables/T-02/seat");
        post("/api/v1/tables/T-02/depart");

        client().post().uri("/api/v1/tables/T-02/seat")
                .exchange()
                .expectStatus().isEqualTo(409)
                .expectBody()
                .jsonPath("$.error").isEqualTo("invalid_transition")
                .jsonPath("$.status").isEqualTo(409)
                .jsonPath("$.message").value(message ->
                        assertThat((String) message).contains("UNCLEANED -> SEATED"))
                .jsonPath("$.details").value(details ->
                        assertThat(details.toString()).contains("currentState=UNCLEANED"))
                .jsonPath("$.path").isEqualTo("/api/v1/tables/T-02/seat")
                .jsonPath("$.timestamp").exists();

        assertThat(get("/api/v1/tables/T-02").state().name())
                .as("a refused command must leave the table exactly as it was")
                .isEqualTo("UNCLEANED");
    }

    @Test
    @DisplayName("treats a repeated assertion as idempotent rather than as a conflict")
    void repeatedAssertionIsIdempotent() {
        TableSnapshot first = post("/api/v1/tables/T-03/seat?occupancy=2");

        TableSnapshot second = post("/api/v1/tables/T-03/seat?occupancy=2");

        assertThat(second.version())
                .as("a duplicate camera assertion must not bump the version or re-arm the SLA")
                .isEqualTo(first.version());
        assertThat(second.slaDeadline()).isEqualTo(first.slaDeadline());
    }

    @Test
    @DisplayName("answers 404 for a table that is not part of the floor plan")
    void unknownTableIsNotFound() {
        client().get().uri("/api/v1/tables/T-999")
                .exchange()
                .expectStatus().isNotFound()
                .expectBody()
                .jsonPath("$.error").isEqualTo("table_not_found")
                .jsonPath("$.message").value(message ->
                        assertThat((String) message).contains("T-999"));

        client().post().uri("/api/v1/tables/T-999/seat")
                .exchange()
                .expectStatus().isNotFound();
    }

    @Test
    @DisplayName("validates the generic signal endpoint and accepts a well-formed body")
    void validatesSignalBody() {
        client().post().uri("/api/v1/tables/T-04/signals")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"occupancy\":2}")
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.error").isEqualTo("validation_failed")
                .jsonPath("$.details").value(details ->
                        assertThat(details.toString()).contains("signal"));

        client().post().uri("/api/v1/tables/T-04/signals")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new SignalRequest(SignalType.GUEST_SEATED, 4, "waiter-2", null))
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.state").isEqualTo("SEATED")
                .jsonPath("$.occupancy").isEqualTo(4)
                .jsonPath("$.colourName").isEqualTo("RED")
                .jsonPath("$.lastSignalSource").isEqualTo("MANUAL_TAP");
    }

    @Test
    @DisplayName("rejects an out-of-range occupancy")
    void rejectsOutOfRangeOccupancy() {
        client().post().uri("/api/v1/tables/T-05/signals")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"signal\":\"GUEST_SEATED\",\"occupancy\":-3}")
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.details").value(details ->
                        assertThat(details.toString()).contains("occupancy"));
    }

    @Test
    @DisplayName("stores the snapshot and its version in the configured Redis key space")
    void persistsToTheConfiguredKeySpace() {
        post("/api/v1/tables/T-06/seat?occupancy=5");

        String json = redis().<String, String>opsForHash()
                .get(properties().redis().tablesKey(), "T-06")
                .block(Duration.ofSeconds(10));
        String version = redis().<String, String>opsForHash()
                .get(properties().redis().tableVersionsKey(), "T-06")
                .block(Duration.ofSeconds(10));

        assertThat(json).isNotNull().contains("\"state\":\"SEATED\"").contains("\"occupancy\":5");
        assertThat(version)
                .as("the Lua compare-and-set must keep the version hash in step with the snapshot")
                .isEqualTo("1");

        Long armed = redis().opsForZSet()
                .size(properties().redis().slaDeadlineIndexKey())
                .block(Duration.ofSeconds(10));
        assertThat(armed).isEqualTo(1L);
    }

    @Test
    @DisplayName("reports provisioned tables, armed timers and ingestion sources through diagnostics")
    void exposesDiagnostics() {
        post("/api/v1/tables/T-07/seat");

        client().get().uri("/api/v1/diagnostics")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.provisionedTables").value(count ->
                        assertThat(((Number) count).longValue()).isGreaterThanOrEqualTo(12L))
                .jsonPath("$.armedSlaTimers").isEqualTo(1)
                .jsonPath("$.ingestionRunning").isEqualTo(true)
                .jsonPath("$.ingestionSources").value(sources ->
                        assertThat(sources.toString()).contains("cctv-motion").contains("manual-tap"));
    }

    @Test
    @DisplayName("reports a healthy readiness probe once Redis is reachable")
    void readinessProbeIsHealthy() {
        client().get().uri("/actuator/health/readiness")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.status").isEqualTo("UP");
    }

    @Test
    @DisplayName("accepts a fire-and-forget ingestion signal with 202")
    void acceptsAsynchronousIngestion() {
        client().post().uri("/api/v1/ingest/signals")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("""
                        {"tableId":"T-08","signal":"GUEST_SEATED","occupancy":2,
                         "actor":"waiter-phone","source":"MOBILE_CAMERA"}""")
                .exchange()
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.status").isEqualTo("accepted")
                .jsonPath("$.tableId").isEqualTo("T-08");

        org.awaitility.Awaitility.await()
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> assertThat(get("/api/v1/tables/T-08").state().name())
                        .as("the ingestion gateway drives the signal through asynchronously")
                        .isEqualTo("SEATED"));
    }

    private TableSnapshot post(String uri) {
        return Objects.requireNonNull(client().post().uri(uri)
                .exchange()
                .expectStatus().isOk()
                .expectBody(TableSnapshot.class)
                .returnResult()
                .getResponseBody());
    }

    private TableSnapshot get(String uri) {
        return Objects.requireNonNull(client().get().uri(uri)
                .exchange()
                .expectStatus().isOk()
                .expectBody(TableSnapshot.class)
                .returnResult()
                .getResponseBody());
    }
}
