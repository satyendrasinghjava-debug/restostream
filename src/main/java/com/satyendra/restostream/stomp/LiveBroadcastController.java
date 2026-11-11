/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.stomp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.satyendra.restostream.broadcast.BroadcastKind;
import com.satyendra.restostream.config.RestoStreamProperties;
import com.satyendra.restostream.domain.OccupancySignal;
import com.satyendra.restostream.domain.SignalType;
import com.satyendra.restostream.domain.TableSnapshot;
import com.satyendra.restostream.engine.InvalidTransitionException;
import com.satyendra.restostream.service.ConcurrencyExhaustedException;
import com.satyendra.restostream.service.SignalApplicationResult;
import com.satyendra.restostream.service.TableManagementService;
import com.satyendra.restostream.service.TableNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>The live broadcaster's STOMP controller — the application half of the WebSocket endpoint.</p>
 *
 * <p>It does exactly two things, and they are the two things the specification asks of a live
 * dashboard channel:</p>
 *
 * <ol>
 *   <li><strong>Primes a new subscriber.</strong> On {@code SUBSCRIBE} it immediately pushes current
 *       state to that session alone. Without this, a dashboard opened at 19:00 would render a blank
 *       floor plan until the next table happened to change — the single most common complaint about
 *       naive push-only dashboards.</li>
 *   <li><strong>Accepts manual taps.</strong> On {@code SEND} to {@code /app/tables/signal} it
 *       drives the same {@link TableManagementService} entry point the REST API and the CCTV
 *       adapter use, so a tap, an HTTP call and a camera alert are indistinguishable by the time
 *       they reach the state engine. That equivalence is the whole point of the ingestion
 *       abstraction.</li>
 * </ol>
 *
 * <p>Outbound broadcasts are <em>not</em> emitted here. They flow from
 * {@link com.satyendra.restostream.broadcast.DashboardBroadcaster} through
 * {@link StompBroadcastRelay}, so that a table transition reaches every dashboard whether it was
 * caused by a tap, an HTTP call, a camera or an SLA breach on another instance entirely.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@Controller
public class LiveBroadcastController implements StompApplicationGateway {

    private static final Logger LOG = LoggerFactory.getLogger(LiveBroadcastController.class);

    private final TableManagementService tableManagementService;
    private final StompSessionRegistry registry;
    private final ObjectMapper objectMapper;
    private final RestoStreamProperties.Stomp config;
    private final Clock clock;

    /**
     * Creates the controller.
     *
     * @param tableManagementService the orchestration service commands are driven through
     * @param registry               the STOMP routing table used for session-scoped delivery
     * @param objectMapper           the application {@link ObjectMapper}
     * @param properties             application configuration supplying destination names
     * @param clock                  the application clock
     */
    public LiveBroadcastController(TableManagementService tableManagementService,
                                   StompSessionRegistry registry,
                                   ObjectMapper objectMapper,
                                   RestoStreamProperties properties,
                                   Clock clock) {
        this.tableManagementService =
                Objects.requireNonNull(tableManagementService, "tableManagementService must not be null");
        this.registry = Objects.requireNonNull(registry, "registry must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.config = Objects.requireNonNull(properties, "properties must not be null").stomp();
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    @Override
    public Mono<Void> onSubscribed(StompClientSession session,
                                   String subscriptionId,
                                   String destination) {
        Objects.requireNonNull(session, "session must not be null");
        Objects.requireNonNull(destination, "destination must not be null");

        String tablesTopic = config.tablesTopic();
        String perTablePrefix = tablesTopic + "/";

        if (StompClientSession.matches(destination, tablesTopic)) {
            return primeWithFloorPlan(session, subscriptionId, destination);
        }
        if (destination.startsWith(perTablePrefix)) {
            if (StompClientSession.isPattern(destination)) {
                return primeWithFloorPlan(session, subscriptionId, destination);
            }
            String tableId = destination.substring(perTablePrefix.length());
            return primeWithTable(session, subscriptionId, destination, tableId);
        }
        // /topic/sla-alerts, /topic/timers and /topic/command-results carry only live events; there
        // is no meaningful "current value" to prime a subscriber with.
        return Mono.empty();
    }

    @Override
    public Mono<Void> onSend(StompClientSession session, String destination, byte[] body) {
        Objects.requireNonNull(session, "session must not be null");
        Objects.requireNonNull(destination, "destination must not be null");

        if (config.floorRefreshDestination().equals(destination)) {
            return refreshFloorPlan(session);
        }
        if (config.signalCommandDestination().equals(destination)) {
            return handleSignalCommand(session, body);
        }
        LOG.debug("session {} addressed unknown application destination {}",
                session.sessionId(), destination);
        return reply(session, CommandResult.failed(null, null,
                "unknown application destination '" + destination + "'", clock.instant()));
    }

    private Mono<Void> handleSignalCommand(StompClientSession session, byte[] body) {
        TableCommand command;
        try {
            command = objectMapper.readValue(body, TableCommand.class);
        } catch (java.io.IOException ex) {
            LOG.debug("session {} sent an undecodable table command", session.sessionId(), ex);
            return reply(session, CommandResult.failed(null, null,
                    "command body is not valid JSON: " + ex.getMessage(), clock.instant()));
        }
        if (command == null || command.tableId() == null || command.tableId().isBlank()) {
            return reply(session, CommandResult.failed(
                    command == null ? null : command.correlationId(), null,
                    "command must carry a non-blank 'tableId'", clock.instant()));
        }
        if (command.signal() == null) {
            return reply(session, CommandResult.failed(command.correlationId(), command.tableId(),
                    "command must carry a 'signal'; one of " + List.of(SignalType.values()),
                    clock.instant()));
        }

        OccupancySignal signal = toSignal(command);
        return tableManagementService.applySignal(signal)
                .flatMap(result -> reply(session, toResult(command, result)))
                .onErrorResume(error -> reply(session, toFailure(command, error)));
    }

    private OccupancySignal toSignal(TableCommand command) {
        return new OccupancySignal(
                UUID.randomUUID().toString(),
                command.tableId(),
                command.signal(),
                command.effectiveSource(),
                command.effectiveActor(),
                clock.instant(),
                1.0d,
                command.effectiveOccupancy(),
                Map.of("transport", "stomp"));
    }

    private CommandResult toResult(TableCommand command, SignalApplicationResult result) {
        return switch (result.decision()) {
            case com.satyendra.restostream.engine.TransitionDecision.Rejected rejected ->
                    CommandResult.rejected(command.correlationId(), command.tableId(),
                            rejected.narrative(), clock.instant());
            case com.satyendra.restostream.engine.TransitionDecision.Applied applied ->
                    CommandResult.accepted(command, applied.next(), true, clock.instant());
            case com.satyendra.restostream.engine.TransitionDecision.Ignored duplicate ->
                    CommandResult.accepted(command, duplicate.current(), false, clock.instant());
        };
    }

    private CommandResult toFailure(TableCommand command, Throwable error) {
        Instant now = clock.instant();
        if (error instanceof TableNotFoundException notFound) {
            return CommandResult.failed(command.correlationId(), notFound.tableId(),
                    notFound.getMessage(), now);
        }
        if (error instanceof InvalidTransitionException invalid) {
            return CommandResult.rejected(command.correlationId(), invalid.tableId(),
                    invalid.getMessage(), now);
        }
        if (error instanceof ConcurrencyExhaustedException contended) {
            return CommandResult.failed(command.correlationId(), contended.tableId(),
                    contended.getMessage(), now);
        }
        LOG.error("table command for {} failed unexpectedly", command.tableId(), error);
        return CommandResult.failed(command.correlationId(), command.tableId(),
                "the command could not be processed: " + error.getClass().getSimpleName(), now);
    }

    private Mono<Void> primeWithFloorPlan(StompClientSession session,
                                          String subscriptionId,
                                          String destination) {
        return tableManagementService.findFloorPlan()
                .collectList()
                .flatMap(floor -> deliver(session, subscriptionId, destination,
                        BroadcastKind.FLOOR_SNAPSHOT, floor))
                .doOnSuccess(ignored -> LOG.debug(
                        "primed session {} subscription '{}' with the floor plan",
                        session.sessionId(), subscriptionId));
    }

    private Mono<Void> primeWithTable(StompClientSession session,
                                      String subscriptionId,
                                      String destination,
                                      String tableId) {
        return tableManagementService.findTable(tableId)
                .flatMap(snapshot -> deliver(session, subscriptionId, destination,
                        BroadcastKind.TABLE_STATE, snapshot))
                .onErrorResume(TableNotFoundException.class, notFound -> {
                    LOG.debug("session {} subscribed to unprovisioned table {}",
                            session.sessionId(), tableId);
                    return Mono.empty();
                });
    }

    private Mono<Void> refreshFloorPlan(StompClientSession session) {
        String tablesTopic = config.tablesTopic();
        return tableManagementService.findFloorPlan()
                .collectList()
                .flatMap((List<TableSnapshot> floor) -> {
                    List<String> subscriptionIds = session.subscriptionsMatching(tablesTopic);
                    if (subscriptionIds.isEmpty()) {
                        return reply(session, CommandResult.failed(null, null,
                                "subscribe to " + tablesTopic + " before requesting a refresh",
                                clock.instant()));
                    }
                    return serialize(floor).flatMap(body -> Mono.<Void>fromRunnable(() ->
                            registry.sendTo(session.sessionId(), tablesTopic,
                                    BroadcastKind.FLOOR_SNAPSHOT, body)));
                })
                .then();
    }

    private Mono<Void> deliver(StompClientSession session,
                               String subscriptionId,
                               String destination,
                               BroadcastKind kind,
                               Object payload) {
        return serialize(payload).flatMap(body -> Mono.<Void>fromRunnable(() -> {
            byte[] frame = StompFrameWriter.message(
                    destination, subscriptionId, session.nextMessageId(), kind.name(), body);
            session.send(frame);
        }));
    }

    private Mono<Void> reply(StompClientSession session, CommandResult result) {
        String destination = config.commandResultsTopic();
        return serialize(result).flatMap(body -> Mono.<Void>fromRunnable(() -> {
            int delivered = registry.sendTo(
                    session.sessionId(), destination, BroadcastKind.TABLE_STATE, body);
            if (delivered == 0) {
                LOG.debug("session {} is not subscribed to {}; command result '{}' was dropped",
                        session.sessionId(), destination, result.status());
            }
        }));
    }

    private Mono<byte[]> serialize(Object payload) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsBytes(payload))
                .onErrorResume(error -> {
                    LOG.error("could not serialise a STOMP payload of type {}",
                            payload.getClass().getSimpleName(), error);
                    return Mono.empty();
                });
    }

    /**
     * Convenience accessor used by the REST layer's diagnostics endpoint.
     *
     * @return the number of dashboards currently attached over STOMP
     */
    public int attachedDashboards() {
        return registry.sessionCount();
    }

    /**
     * Returns the current state of one table, used by the diagnostics endpoint to verify that the
     * broker and the state store agree.
     *
     * @param tableId the table identifier
     * @return the table snapshot
     */
    public Mono<TableSnapshot> peek(String tableId) {
        return tableManagementService.findTable(tableId);
    }
}
