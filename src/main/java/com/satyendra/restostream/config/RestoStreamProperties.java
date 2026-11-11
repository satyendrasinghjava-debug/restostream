/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.config;

import com.satyendra.restostream.domain.SlaKind;
import com.satyendra.restostream.engine.SlaPolicy;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>The single, strongly-typed, immutable binding of every externalised RestoStream setting.
 * Implemented as nested records with constructor binding so that configuration is validated once at
 * startup and can then be passed around as a value — no {@code @Value} strings scattered across
 * the codebase and no possibility of a mistyped key silently defaulting to zero at runtime.</p>
 *
 * <p>The type also implements {@link SlaPolicy}, which is how the pure state engine learns the
 * operator's timeouts without ever being coupled to Spring.</p>
 *
 * @param redis     Redis key-space layout
 * @param sla       service-level agreement timeouts
 * @param timers    distributed timer subsystem tuning
 * @param ingestion signal ingestion tuning, including the CCTV motion conditioner
 * @param stomp     reactive STOMP endpoint and destination configuration
 * @param layout    the floor plan provisioned at startup
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@ConfigurationProperties(prefix = "restostream")
public record RestoStreamProperties(
        @DefaultValue Redis redis,
        @DefaultValue Sla sla,
        @DefaultValue Timers timers,
        @DefaultValue Ingestion ingestion,
        @DefaultValue Stomp stomp,
        @DefaultValue Layout layout) implements SlaPolicy {

    @Override
    public Optional<Duration> timeoutFor(SlaKind slaKind) {
        if (slaKind == null) {
            return Optional.empty();
        }
        Duration configured = switch (slaKind) {
            case WAITER_RESPONSE -> sla.waiterResponse();
            case TABLE_TURNAROUND -> sla.tableTurnaround();
        };
        if (configured == null || configured.isZero() || configured.isNegative()) {
            return Optional.empty();
        }
        return Optional.of(configured);
    }

    /**
     * Redis key-space layout. Every key RestoStream writes is derived from {@code keyPrefix}, so a
     * single Redis instance can safely host several restaurants or environments.
     *
     * @param keyPrefix root namespace for all keys
     * @param database  the Redis logical database index, used to build the keyspace-event channel
     */
    public record Redis(@DefaultValue("restostream") String keyPrefix,
                        @DefaultValue("0") int database) {

        /** @return the hash holding table snapshots as JSON */
        public String tablesKey() {
            return keyPrefix + ":tables";
        }

        /** @return the hash holding per-table optimistic-concurrency versions */
        public String tableVersionsKey() {
            return keyPrefix + ":tables:v";
        }

        /** @return prefix of the per-table, TTL-bearing SLA timer keys */
        public String slaTimerKeyPrefix() {
            return keyPrefix + ":sla:timer:";
        }

        /**
         * Builds the TTL-bearing timer key for one table.
         *
         * @param tableId the table; must not be {@code null}
         * @return the fully-qualified Redis key
         */
        public String slaTimerKey(String tableId) {
            return slaTimerKeyPrefix() + tableId;
        }

        /**
         * Extracts the table identifier from a timer key received in a keyspace-expiry event.
         *
         * @param key the expired Redis key
         * @return the table identifier, or {@link Optional#empty()} when the key is not one of ours
         */
        public Optional<String> tableIdFromSlaTimerKey(String key) {
            String prefix = slaTimerKeyPrefix();
            if (key == null || !key.startsWith(prefix) || key.length() == prefix.length()) {
                return Optional.empty();
            }
            return Optional.of(key.substring(prefix.length()));
        }

        /** @return the sorted set indexing armed SLA deadlines by epoch millisecond */
        public String slaDeadlineIndexKey() {
            return keyPrefix + ":sla:deadlines";
        }

        /** @return the hash holding the SLA kind for each armed timer */
        public String slaMetaKey() {
            return keyPrefix + ":sla:meta";
        }
    }

    /**
     * Service-level agreement timeouts. A zero or negative duration switches that SLA off, which the
     * engine treats as an instruction to cancel any armed timer rather than to leave it running.
     *
     * @param waiterResponse  how long a seated table may go unattended; the five-minute SLA the
     *                        RestoStream specification is built around
     * @param tableTurnaround how long a vacated table may go un-bussed
     */
    public record Sla(@DefaultValue("5m") Duration waiterResponse,
                      @DefaultValue("3m") Duration tableTurnaround) {
    }

    /**
     * Distributed timer subsystem tuning.
     *
     * @param keyspaceNotificationsEnabled whether to subscribe to Redis key-expiry events for
     *                                     sub-second breach detection
     * @param keyspacePattern              the pub/sub pattern expiry events arrive on
     * @param sweepInterval                cadence of the reconciliation sweep that guarantees
     *                                     delivery when a notification is missed
     * @param sweepBatchSize               maximum overdue deadlines claimed per sweep
     * @param sweepEnabled                 whether the reconciliation sweep runs at all
     * @param countdownEnabled             whether live SLA countdown ticks are broadcast
     * @param countdownInterval            cadence of the countdown tick broadcast
     * @param driftReconcileEnabled        whether timer drift between the snapshot store and the
     *                                     deadline index is repaired periodically
     * @param driftReconcileInterval       cadence of the drift reconciliation pass
     */
    public record Timers(@DefaultValue("true") boolean keyspaceNotificationsEnabled,
                         @DefaultValue("__keyevent@*__:expired") String keyspacePattern,
                         @DefaultValue("1s") Duration sweepInterval,
                         @DefaultValue("256") int sweepBatchSize,
                         @DefaultValue("true") boolean sweepEnabled,
                         @DefaultValue("true") boolean countdownEnabled,
                         @DefaultValue("5s") Duration countdownInterval,
                         @DefaultValue("true") boolean driftReconcileEnabled,
                         @DefaultValue("30s") Duration driftReconcileInterval) {
    }

    /**
     * Signal ingestion tuning.
     *
     * @param bufferSize           per-source buffer depth before backpressure is applied upstream
     * @param concurrencyPerSource in-flight signals permitted per source
     * @param housekeepingInterval cadence of {@code OccupancySignalSource.housekeep}
     * @param cctv                 the CCTV motion conditioner configuration
     */
    public record Ingestion(@DefaultValue("512") int bufferSize,
                            @DefaultValue("16") int concurrencyPerSource,
                            @DefaultValue("5m") Duration housekeepingInterval,
                            @DefaultValue Cctv cctv) {
    }

    /**
     * CCTV motion conditioner configuration. These six numbers are the entire occupancy-detection
     * "model" — classical signal conditioning, no inference.
     *
     * @param enabled             whether the CCTV adapter participates in ingestion
     * @param sourceId            identifier used in logs, metrics and signal provenance
     * @param occupiedMotionRatio lower bound of the occupied band
     * @param vacantMotionRatio   upper bound of the vacant band
     * @param confirmationFrames  consecutive qualifying alerts required to confirm a judgement
     * @param minConfidence       minimum edge confidence an alert must carry
     * @param emitDebounce        minimum interval between two emitted signals for one table
     * @param trackRetention      how long per-table evidence survives without new alerts
     */
    public record Cctv(@DefaultValue("true") boolean enabled,
                       @DefaultValue("cctv-motion") String sourceId,
                       @DefaultValue("0.18") double occupiedMotionRatio,
                       @DefaultValue("0.04") double vacantMotionRatio,
                       @DefaultValue("3") int confirmationFrames,
                       @DefaultValue("0.45") double minConfidence,
                       @DefaultValue("20s") Duration emitDebounce,
                       @DefaultValue("30m") Duration trackRetention) {
    }

    /**
     * Reactive STOMP endpoint configuration.
     *
     * @param endpoint            HTTP path the WebSocket handshake is served on
     * @param applicationPrefix   destination prefix for client-to-server {@code SEND} frames
     * @param topicPrefix         destination prefix for server-to-client broadcasts
     * @param outboundBufferSize  per-session outbound frame buffer depth
     * @param heartbeatSend       interval at which the server promises to send heart-beats
     * @param heartbeatReceive    interval at which the server expects client heart-beats
     * @param sessionIdleTimeout  how long a session may be silent before it is closed
     */
    public record Stomp(@DefaultValue("/ws/stomp") String endpoint,
                        @DefaultValue("/app") String applicationPrefix,
                        @DefaultValue("/topic") String topicPrefix,
                        @DefaultValue("256") int outboundBufferSize,
                        @DefaultValue("10s") Duration heartbeatSend,
                        @DefaultValue("10s") Duration heartbeatReceive,
                        @DefaultValue("5m") Duration sessionIdleTimeout) {

        /** @return destination carrying every table state change */
        public String tablesTopic() {
            return topicPrefix + "/tables";
        }

        /**
         * Builds the per-table destination.
         *
         * @param tableId the table; must not be {@code null}
         * @return the destination a dashboard subscribes to for one table only
         */
        public String tableTopic(String tableId) {
            return topicPrefix + "/tables/" + tableId;
        }

        /** @return destination carrying SLA breach alerts */
        public String slaAlertsTopic() {
            return topicPrefix + "/sla-alerts";
        }

        /** @return destination carrying the periodic countdown tick for armed SLA timers */
        public String timersTopic() {
            return topicPrefix + "/timers";
        }

        /**
         * Destination on which a dashboard receives the result of its own commands. Delivered only
         * to the session that issued the command, never broadcast.
         *
         * @return the command-result destination
         */
        public String commandResultsTopic() {
            return topicPrefix + "/command-results";
        }

        /** @return application destination accepting occupancy-signal commands */
        public String signalCommandDestination() {
            return applicationPrefix + "/tables/signal";
        }

        /** @return application destination requesting a fresh floor snapshot */
        public String floorRefreshDestination() {
            return applicationPrefix + "/floor/refresh";
        }
    }

    /**
     * The floor plan provisioned into Redis at startup. Provisioning is idempotent: an existing
     * table is never overwritten, so restarting the service does not reset a live dining room.
     *
     * @param provisionOnStartup whether to provision the configured tables at startup
     * @param tables             the tables that make up the floor plan; empty when unconfigured
     */
    public record Layout(@DefaultValue("true") boolean provisionOnStartup,
                         @DefaultValue List<TableDefinition> tables) {

        /** Canonical constructor normalising an absent table list to an empty one. */
        public Layout {
            tables = tables == null ? List.of() : List.copyOf(tables);
        }
    }

    /**
     * One table in the floor plan.
     *
     * @param id    stable business identifier, e.g. {@code T-01}
     * @param zone  dining area
     * @param seats seat capacity
     */
    public record TableDefinition(String id,
                                  @DefaultValue("DEFAULT") String zone,
                                  @DefaultValue("2") int seats) {
    }
}
