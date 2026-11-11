/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.web;

import com.satyendra.restostream.broadcast.BroadcastEnvelope;
import com.satyendra.restostream.broadcast.DashboardBroadcaster;
import com.satyendra.restostream.domain.OccupancySignal;
import com.satyendra.restostream.domain.SignalSourceKind;
import com.satyendra.restostream.domain.SignalType;
import com.satyendra.restostream.domain.TableSnapshot;
import com.satyendra.restostream.service.TableManagementService;
import com.satyendra.restostream.web.dto.SignalRequest;
import jakarta.validation.Valid;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>The synchronous command and query surface for the floor plan — fully reactive, returning
 * {@link Mono} and {@link Flux} so that no request ever parks a thread.</p>
 *
 * <h2>Two styles of write, on purpose</h2>
 * <p>{@code POST /{tableId}/signals} is the general form, and the four named endpoints
 * ({@code /seat}, {@code /depart}, {@code /waiter-ack}, {@code /cleaned}) are thin aliases over it.
 * The aliases exist because a dashboard tile's button should be a single zero-body {@code POST}, not
 * a JSON document a front-end developer can mistype into a {@code 400}. Both styles converge on the
 * same {@link TableManagementService} entry point, so there is exactly one write path to reason
 * about.</p>
 *
 * <h2>Why these are synchronous while CCTV ingestion is not</h2>
 * <p>A waiter tapping "seated" is waiting for the tile to turn red and needs to be told if the
 * table was already occupied — so these endpoints return the resulting state, or {@code 409}. A
 * camera is not waiting for anything, so {@link IngestionController} accepts and returns
 * {@code 202} immediately. Matching the response contract to who is actually waiting is what keeps
 * the camera path fast and the tap path informative.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@RestController
@RequestMapping("/api/v1/tables")
public class TableRestController {

    private final TableManagementService tableManagementService;
    private final DashboardBroadcaster broadcaster;
    private final Clock clock;

    /**
     * Creates the controller.
     *
     * @param tableManagementService the orchestration service
     * @param broadcaster            the fan-out, used to back the Server-Sent Events stream
     * @param clock                  the application clock
     */
    public TableRestController(TableManagementService tableManagementService,
                               DashboardBroadcaster broadcaster,
                               Clock clock) {
        this.tableManagementService =
                Objects.requireNonNull(tableManagementService, "tableManagementService must not be null");
        this.broadcaster = Objects.requireNonNull(broadcaster, "broadcaster must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    /**
     * Returns the whole floor plan, ordered by table identifier.
     *
     * @return every provisioned table
     */
    @GetMapping
    public Flux<TableSnapshot> floorPlan() {
        return tableManagementService.findFloorPlan();
    }

    /**
     * Returns one table.
     *
     * @param tableId the table identifier
     * @return the table snapshot, or {@code 404} when unprovisioned
     */
    @GetMapping("/{tableId}")
    public Mono<TableSnapshot> table(@PathVariable String tableId) {
        return tableManagementService.findTable(tableId);
    }

    /**
     * Raises an arbitrary occupancy signal against a table.
     *
     * @param tableId the table identifier
     * @param request the signal to raise
     * @return the resulting table state, {@code 404} when unprovisioned, or {@code 409} when the
     *         state machine refuses the transition
     */
    @PostMapping("/{tableId}/signals")
    public Mono<TableSnapshot> raiseSignal(@PathVariable String tableId,
                                           @Valid @RequestBody SignalRequest request) {
        return apply(tableId, request.signal(), request.effectiveOccupancy(),
                request.effectiveActor(), request.effectiveSource());
    }

    /**
     * Marks a table as seated — the primary trigger, which arms the waiter-response SLA timer.
     *
     * @param tableId   the table identifier
     * @param occupancy guest headcount, when known
     * @param actor     identifier of the waiter or device
     * @return the resulting table state
     */
    @PostMapping("/{tableId}/seat")
    public Mono<TableSnapshot> seat(@PathVariable String tableId,
                                    @RequestParam(name = "occupancy", required = false, defaultValue = "0") int occupancy,
                                    @RequestParam(name = "actor", required = false) String actor) {
        return apply(tableId, SignalType.GUEST_SEATED, occupancy,
                actor == null ? "rest-api" : actor, SignalSourceKind.MANUAL_TAP);
    }

    /**
     * Marks a table's guests as departed, moving it to {@code UNCLEANED}.
     *
     * @param tableId the table identifier
     * @param actor   identifier of the waiter or device
     * @return the resulting table state
     */
    @PostMapping("/{tableId}/depart")
    public Mono<TableSnapshot> depart(@PathVariable String tableId,
                                      @RequestParam(name = "actor", required = false) String actor) {
        return apply(tableId, SignalType.GUEST_DEPARTED, 0,
                actor == null ? "rest-api" : actor, SignalSourceKind.MANUAL_TAP);
    }

    /**
     * Records a waiter acknowledgement, which cancels the armed SLA timer. This is the "waiter
     * event" whose absence for five minutes triggers an alert.
     *
     * @param tableId the table identifier
     * @param actor   identifier of the waiter
     * @return the resulting table state
     */
    @PostMapping("/{tableId}/waiter-ack")
    public Mono<TableSnapshot> waiterAcknowledged(@PathVariable String tableId,
                                                  @RequestParam(name = "actor", required = false) String actor) {
        return apply(tableId, SignalType.WAITER_ATTENDED, 0,
                actor == null ? "rest-api" : actor, SignalSourceKind.MANUAL_TAP);
    }

    /**
     * Records that bussing is complete, returning the table to {@code FREE}.
     *
     * @param tableId the table identifier
     * @param actor   identifier of the staff member or device
     * @return the resulting table state
     */
    @PostMapping("/{tableId}/cleaned")
    public Mono<TableSnapshot> cleaned(@PathVariable String tableId,
                                       @RequestParam(name = "actor", required = false) String actor) {
        return apply(tableId, SignalType.CLEANING_COMPLETED, 0,
                actor == null ? "rest-api" : actor, SignalSourceKind.MANUAL_TAP);
    }

    /**
     * Server-Sent Events mirror of the STOMP broadcast stream.
     *
     * <p>Provided alongside STOMP because the two serve different consumers: dashboards want STOMP's
     * destination-scoped subscriptions, while operators, smoke tests and {@code curl} want a stream
     * they can read with no client library at all. Both are fed by the same
     * {@link DashboardBroadcaster}, so neither can drift from the other.</p>
     *
     * @return an infinite stream of broadcast events, with periodic keep-alive comments
     */
    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<Object>> stream() {
        Flux<ServerSentEvent<Object>> events = broadcaster.envelopes()
                .map(TableRestController::toServerSentEvent);
        Flux<ServerSentEvent<Object>> keepAlive = Flux.interval(Duration.ofSeconds(15))
                .map(tick -> ServerSentEvent.<Object>builder()
                        .comment("keep-alive")
                        .build());
        return Flux.merge(events, keepAlive);
    }

    private static ServerSentEvent<Object> toServerSentEvent(BroadcastEnvelope envelope) {
        return ServerSentEvent.<Object>builder()
                .event(envelope.kind().name())
                .id(Long.toString(envelope.emittedAt().toEpochMilli()))
                .comment(String.join(",", envelope.destinations()))
                .data(envelope.payload())
                .build();
    }

    private Mono<TableSnapshot> apply(String tableId,
                                      SignalType type,
                                      int occupancy,
                                      String actor,
                                      SignalSourceKind source) {
        return Mono.fromSupplier(() -> new OccupancySignal(
                        UUID.randomUUID().toString(),
                        tableId,
                        type,
                        source,
                        actor,
                        clock.instant(),
                        1.0d,
                        occupancy,
                        Map.of("transport", "rest")))
                .flatMap(tableManagementService::applySignalOrFail);
    }
}
