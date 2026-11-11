/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.satyendra.restostream.domain.TableSnapshot;
import com.satyendra.restostream.ingestion.MotionIngestOutcome;
import com.satyendra.restostream.support.RedisIntegrationTestBase;
import com.satyendra.restostream.support.StompTestClient;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.MediaType;
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
 * <p>End-to-end integration suite for the CCTV ingestion path — the seam that lets a video pipeline
 * replace manual taps without touching anything downstream.</p>
 *
 * <p>The alerts posted here are the same payload an NVR or an OpenCV background-subtractor would
 * post. Nothing in the state engine, the timer subsystem or the broadcaster can tell the difference
 * between this and a waiter tapping a tile, which is precisely the property these tests exist to
 * prove.</p>
 *
 * <p>The emission debounce is compressed to a quarter of a second so that a reversal can be driven
 * inside a test; the thresholds, the dead band and the confirmation streak are left at their
 * production values.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DisplayName("CCTV motion ingestion (integration)")
class CctvIngestionIntegrationTest extends RedisIntegrationTestBase {

    private static final Duration TIMEOUT = Duration.ofSeconds(25);

    /**
     * Compresses only the debounce window, so that reversals can be exercised while the detection
     * rules themselves stay at production values.
     *
     * @param registry the dynamic property registry
     */
    @DynamicPropertySource
    static void compressDebounce(DynamicPropertyRegistry registry) {
        registry.add("restostream.ingestion.cctv.emit-debounce", () -> "250ms");
        registry.add("restostream.sla.waiter-response", () -> "10m");
        registry.add("restostream.sla.table-turnaround", () -> "10m");
    }

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
    @DisplayName("seats a table after three confirming motion alerts and broadcasts the change")
    void threeConfirmingAlertsSeatTheTable() {
        dashboard.subscribe("/topic/tables");
        dashboard.awaitMessage("/topic/tables", "FLOOR_SNAPSHOT");

        assertThat(motion("T-01", 0.42d, 0.9d).disposition())
                .isEqualTo(MotionIngestOutcome.Disposition.AWAITING_CONFIRMATION);
        assertThat(motion("T-01", 0.42d, 0.9d).disposition())
                .isEqualTo(MotionIngestOutcome.Disposition.AWAITING_CONFIRMATION);

        MotionIngestOutcome third = motion("T-01", 0.42d, 0.9d);
        assertThat(third.disposition()).isEqualTo(MotionIngestOutcome.Disposition.EMITTED);
        assertThat(third.emittedSignal().name()).isEqualTo("GUEST_SEATED");

        StompTestClient.Frame frame = dashboard.awaitMessage("/topic/tables",
                body -> body.contains("\"tableId\":\"T-01\"") && body.contains("\"state\":\"SEATED\""),
                TIMEOUT);
        TableSnapshot table = frame.bodyAs(objectMapper, TableSnapshot.class);

        assertThat(table.state().name()).isEqualTo("SEATED");
        assertThat(table.lastSignalSource().name())
                .as("provenance must survive all the way to the dashboard")
                .isEqualTo("CCTV_MOTION");
        assertThat(table.occupancy())
                .as("the cheap blob count becomes the guest headcount")
                .isEqualTo(2);
        assertThat(table.slaArmed())
                .as("a camera-driven seating arms the same SLA a manual tap would")
                .isTrue();
    }

    @Test
    @DisplayName("ignores a single transient alert, so a passing waiter never seats a table")
    void transientMotionDoesNotSeatTheTable() {
        motion("T-02", 0.55d, 0.9d);
        motion("T-02", 0.01d, 0.9d);
        motion("T-02", 0.55d, 0.9d);
        motion("T-02", 0.01d, 0.9d);

        assertThat(get("T-02").state().name())
                .as("alternating readings reset the streak, so nothing is ever confirmed")
                .isEqualTo("FREE");
        assertThat(get("T-02").version()).isZero();
    }

    @Test
    @DisplayName("ignores readings inside the hysteresis dead band")
    void deadBandIsIgnored() {
        for (int i = 0; i < 6; i++) {
            assertThat(motion("T-03", 0.10d, 0.9d).disposition())
                    .isEqualTo(MotionIngestOutcome.Disposition.AMBIGUOUS);
        }

        assertThat(get("T-03").state().name()).isEqualTo("FREE");
    }

    @Test
    @DisplayName("discards alerts below the confidence floor")
    void lowConfidenceAlertsAreDiscarded() {
        for (int i = 0; i < 6; i++) {
            assertThat(motion("T-04", 0.80d, 0.10d).disposition())
                    .isEqualTo(MotionIngestOutcome.Disposition.LOW_CONFIDENCE);
        }

        assertThat(get("T-04").state().name()).isEqualTo("FREE");
    }

    @Test
    @DisplayName("stays silent for a camera watching an empty table all morning")
    void quietCameraRaisesNothing() {
        for (int i = 0; i < 8; i++) {
            assertThat(motion("T-05", 0.005d, 0.95d).disposition())
                    .isEqualTo(MotionIngestOutcome.Disposition.DUPLICATE_ASSERTION);
        }

        assertThat(get("T-05").state().name()).isEqualTo("FREE");
        assertThat(get("T-05").version())
                .as("a vacant table is assumed at genesis, so no departure is ever asserted")
                .isZero();
    }

    @Test
    @DisplayName("drives a table through seating and departure from the camera feed alone")
    void cameraDrivesTheWholeTransition() {
        confirmOccupied("T-06");
        Awaitility.await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(get("T-06").state().name()).isEqualTo("SEATED"));

        // Let the compressed emission debounce window close, then confirm the table has emptied.
        awaitDebounceWindow();
        confirmVacant("T-06");

        Awaitility.await().atMost(TIMEOUT).untilAsserted(() -> {
            TableSnapshot table = get("T-06");
            assertThat(table.state().name()).isEqualTo("UNCLEANED");
            assertThat(table.colourName()).isEqualTo("AMBER");
            assertThat(table.lastSignalSource().name()).isEqualTo("CCTV_MOTION");
        });
    }

    @Test
    @DisplayName("accepts a batch of alerts across several tables in one request")
    void acceptsABatch() {
        String batch = """
                [
                  {"tableId":"T-07","cameraId":"cam-a","motionRatio":0.40,"occupancyHint":2,"confidence":0.9},
                  {"tableId":"T-07","cameraId":"cam-a","motionRatio":0.40,"occupancyHint":2,"confidence":0.9},
                  {"tableId":"T-07","cameraId":"cam-a","motionRatio":0.40,"occupancyHint":2,"confidence":0.9},
                  {"tableId":"T-08","cameraId":"cam-b","motionRatio":0.10,"occupancyHint":0,"confidence":0.9}
                ]""";

        web.post().uri("/api/v1/ingest/cctv/motion/batch")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(batch)
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(MotionIngestOutcome.class)
                .hasSize(4)
                .consumeWith(result -> {
                    assertThat(Objects.requireNonNull(result.getResponseBody()).get(2).disposition())
                            .isEqualTo(MotionIngestOutcome.Disposition.EMITTED);
                    assertThat(result.getResponseBody().get(3).disposition())
                            .isEqualTo(MotionIngestOutcome.Disposition.AMBIGUOUS);
                });

        Awaitility.await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(get("T-07").state().name()).isEqualTo("SEATED"));
        assertThat(get("T-08").state().name()).isEqualTo("FREE");
    }

    @Test
    @DisplayName("generates an alert identifier and capture instant when the edge omits them")
    void appliesServerSideDefaults() {
        MotionIngestOutcome outcome = Objects.requireNonNull(
                web.post().uri("/api/v1/ingest/cctv/motion")
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue("{\"tableId\":\"T-09\",\"motionRatio\":0.40}")
                        .exchange()
                        .expectStatus().isOk()
                        .expectBody(MotionIngestOutcome.class)
                        .returnResult()
                        .getResponseBody());

        assertThat(outcome.alertId())
                .as("a constrained edge device may post the smallest possible payload")
                .isNotBlank();
        assertThat(outcome.tableId()).isEqualTo("T-09");
    }

    @Test
    @DisplayName("validates a malformed alert with 400 rather than accepting nonsense")
    void validatesMalformedAlert() {
        web.post().uri("/api/v1/ingest/cctv/motion")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"tableId\":\"T-10\",\"motionRatio\":4.2}")
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.error").isEqualTo("validation_failed")
                .jsonPath("$.details").value(details ->
                        assertThat(details.toString()).contains("motionRatio"));

        web.post().uri("/api/v1/ingest/cctv/motion")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"motionRatio\":0.4}")
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.details").value(details ->
                        assertThat(details.toString()).contains("tableId"));
    }

    @Test
    @DisplayName("reports conditioner counters through the CCTV status endpoint")
    void reportsConditionerStatus() {
        confirmOccupied("T-11");

        web.get().uri("/api/v1/ingest/cctv/status")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.sourceId").isEqualTo("cctv-motion")
                .jsonPath("$.enabled").isEqualTo(true)
                .jsonPath("$.trackedTables").value(count ->
                        assertThat(((Number) count).intValue()).isGreaterThanOrEqualTo(1))
                .jsonPath("$.alertsAccepted").value(count ->
                        assertThat(((Number) count).longValue()).isGreaterThanOrEqualTo(3L))
                .jsonPath("$.signalsEmitted").value(count ->
                        assertThat(((Number) count).longValue()).isGreaterThanOrEqualTo(1L));
    }

    /**
     * Waits out the compressed emission debounce window. This is the one place a real wait is
     * unavoidable: the conditioner reads wall-clock time from the application's own clock inside a
     * running Spring context, so there is no clock to advance by hand from out here.
     */
    private static void awaitDebounceWindow() {
        try {
            Thread.sleep(400L);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting out the debounce window",
                    interrupted);
        }
    }

    private void confirmOccupied(String tableId) {
        for (int i = 0; i < 3; i++) {
            motion(tableId, 0.42d, 0.9d);
        }
    }

    private void confirmVacant(String tableId) {
        for (int i = 0; i < 3; i++) {
            motion(tableId, 0.01d, 0.9d);
        }
    }

    private MotionIngestOutcome motion(String tableId, double ratio, double confidence) {
        String body = """
                {"tableId":"%s","cameraId":"cam-itest","zone":"WINDOW","motionRatio":%s,
                 "pixelDelta":%d,"occupancyHint":2,"confidence":%s}"""
                .formatted(tableId, ratio, Math.round(ratio * 90_000), confidence);
        return Objects.requireNonNull(web.post().uri("/api/v1/ingest/cctv/motion")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(body)
                .exchange()
                .expectStatus().isOk()
                .expectBody(MotionIngestOutcome.class)
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
