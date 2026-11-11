/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.broadcast;

import com.satyendra.restostream.alert.SlaAlert;
import com.satyendra.restostream.config.RestoStreamProperties;
import com.satyendra.restostream.domain.TableSnapshot;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Clock;
import java.util.List;
import java.util.Objects;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>The single outbound fan-out point. Everything the restaurant's dashboards see — state changes,
 * SLA alerts, countdown ticks — is published here exactly once, by exactly one call site, and then
 * distributed to every attached transport.</p>
 *
 * <h2>Why a hot multicast sink rather than direct session writes</h2>
 * <p>The naive implementation has the orchestration service iterate over connected WebSocket
 * sessions and write to each. That couples business logic to transport bookkeeping, makes the
 * service untestable without a WebSocket, and means adding Server-Sent Events as a second transport
 * requires editing the service. Here the service publishes one envelope and knows nothing about who
 * is listening; the STOMP broker and the SSE endpoint are independent subscribers.</p>
 *
 * <p>The sink is {@code multicast().onBackpressureBuffer} with {@code autoCancel} disabled, so the
 * stream survives the last dashboard disconnecting and is immediately live for the next one.
 * Replay is deliberately <em>not</em> used: a dashboard that connects mid-service must not be shown
 * a stale backlog of transitions, it must be shown the current floor plan — which is exactly what
 * {@code FLOOR_SNAPSHOT} delivers on subscribe.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@Component
public class DashboardBroadcaster {

    private static final Logger LOG = LoggerFactory.getLogger(DashboardBroadcaster.class);
    private static final int BUFFER_SIZE = 2_048;

    private final RestoStreamProperties.Stomp stomp;
    private final MeterRegistry meterRegistry;
    private final Clock clock;
    private final Sinks.Many<BroadcastEnvelope> sink =
            Sinks.many().multicast().onBackpressureBuffer(BUFFER_SIZE, false);

    /**
     * Creates the broadcaster.
     *
     * @param properties    application configuration supplying destination names
     * @param meterRegistry registry for broadcast counters
     * @param clock         clock stamping each envelope
     */
    public DashboardBroadcaster(RestoStreamProperties properties,
                                MeterRegistry meterRegistry,
                                Clock clock) {
        this.stomp = Objects.requireNonNull(properties, "properties must not be null").stomp();
        this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    /**
     * Broadcasts one table's new state to both the aggregate feed and that table's own feed.
     *
     * @param snapshot the new state; must not be {@code null}
     */
    public void broadcastTableState(TableSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        emit(new BroadcastEnvelope(
                List.of(stomp.tablesTopic(), stomp.tableTopic(snapshot.tableId())),
                BroadcastKind.TABLE_STATE,
                snapshot,
                clock.instant()));
    }

    /**
     * Broadcasts the complete floor plan on the aggregate feed.
     *
     * @param floor every provisioned table; must not be {@code null}
     */
    public void broadcastFloorSnapshot(List<TableSnapshot> floor) {
        Objects.requireNonNull(floor, "floor must not be null");
        emit(new BroadcastEnvelope(
                List.of(stomp.tablesTopic()),
                BroadcastKind.FLOOR_SNAPSHOT,
                List.copyOf(floor),
                clock.instant()));
    }

    /**
     * Broadcasts an SLA breach alert on the alert feed and on the breaching table's own feed.
     *
     * @param alert the alert; must not be {@code null}
     */
    public void broadcastSlaAlert(SlaAlert alert) {
        Objects.requireNonNull(alert, "alert must not be null");
        emit(new BroadcastEnvelope(
                List.of(stomp.slaAlertsTopic(), stomp.tableTopic(alert.tableId())),
                BroadcastKind.SLA_ALERT,
                alert,
                clock.instant()));
    }

    /**
     * Broadcasts a batch of live SLA countdowns on the timer feed.
     *
     * @param ticks the countdowns; must not be {@code null}, may be empty in which case nothing is
     *              broadcast
     */
    public void broadcastTimerTicks(List<TimerTick> ticks) {
        Objects.requireNonNull(ticks, "ticks must not be null");
        if (ticks.isEmpty()) {
            return;
        }
        emit(new BroadcastEnvelope(
                List.of(stomp.timersTopic()),
                BroadcastKind.TIMER_TICK,
                List.copyOf(ticks),
                clock.instant()));
    }

    /**
     * @return the hot stream of broadcast envelopes; never completes during normal operation
     */
    public Flux<BroadcastEnvelope> envelopes() {
        return sink.asFlux();
    }

    private void emit(BroadcastEnvelope envelope) {
        Sinks.EmitResult result = sink.tryEmitNext(envelope);
        Counter.builder("restostream.broadcast.envelopes")
                .description("Envelopes published to the dashboard fan-out")
                .tag("kind", envelope.kind().name())
                .tag("outcome", result.isSuccess() ? "published" : result.name())
                .register(meterRegistry)
                .increment();
        if (!result.isSuccess() && result != Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER) {
            LOG.warn("dashboard fan-out refused a {} envelope for {}: {}",
                    envelope.kind(), envelope.destinations(), result);
        }
    }
}
