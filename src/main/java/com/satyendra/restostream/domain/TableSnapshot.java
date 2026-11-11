/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>The complete, immutable state of one restaurant table at one instant — the aggregate that is
 * persisted in Redis, pushed over STOMP and returned from the REST API. Being a record, it is
 * inherently safe to share across the reactive pipeline without defensive copying.</p>
 *
 * <p>{@code version} implements optimistic concurrency. Two RestoStream instances racing on the
 * same table (a waiter's tap and a CCTV alert arriving simultaneously) both read version
 * {@code n}; only the first compare-and-set to {@code n + 1} succeeds and the loser retries against
 * fresh state. This is what keeps the state machine correct horizontally, not just in one JVM.</p>
 *
 * @param tableId          stable business identifier of the table, e.g. {@code T-12}
 * @param zone             dining area the table belongs to, e.g. {@code PATIO}
 * @param seats            physical seat capacity
 * @param state            current lifecycle state
 * @param stateSince       instant the table entered {@link #state}
 * @param version          optimistic-concurrency version, incremented on every applied transition
 * @param slaKind          the SLA currently guarding the table, or {@code null} when unguarded
 * @param slaDeadline      instant by which the SLA must be satisfied, or {@code null}
 * @param slaBreached      whether the active SLA has already been breached and alerted
 * @param occupancy        guest headcount last observed, {@code 0} when unoccupied or unknown
 * @param lastSignalType   the signal that produced the current state, or {@code null} at genesis
 * @param lastSignalSource the transport that produced the current state, or {@code null}
 * @param lastWaiterAckAt  instant a waiter last acknowledged this table, or {@code null}
 * @param updatedAt        instant this snapshot was produced
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TableSnapshot(
        String tableId,
        String zone,
        int seats,
        TableState state,
        Instant stateSince,
        long version,
        SlaKind slaKind,
        Instant slaDeadline,
        boolean slaBreached,
        int occupancy,
        SignalType lastSignalType,
        SignalSourceKind lastSignalSource,
        Instant lastWaiterAckAt,
        Instant updatedAt) {

    /**
     * Canonical constructor enforcing the aggregate's invariants.
     */
    public TableSnapshot {
        Objects.requireNonNull(tableId, "tableId must not be null");
        Objects.requireNonNull(state, "state must not be null");
        Objects.requireNonNull(stateSince, "stateSince must not be null");
        Objects.requireNonNull(updatedAt, "updatedAt must not be null");
        if (tableId.isBlank()) {
            throw new IllegalArgumentException("tableId must not be blank");
        }
        if (seats < 0) {
            throw new IllegalArgumentException("seats must not be negative but was " + seats);
        }
        if (version < 0L) {
            throw new IllegalArgumentException("version must not be negative but was " + version);
        }
        if (occupancy < 0) {
            throw new IllegalArgumentException("occupancy must not be negative but was " + occupancy);
        }
        zone = zone == null ? "DEFAULT" : zone;
    }

    /**
     * Creates the genesis snapshot for a newly provisioned table.
     *
     * @param tableId stable business identifier
     * @param zone    dining area, may be {@code null} to default
     * @param seats   physical seat capacity
     * @param now     the provisioning instant
     * @return a version-zero snapshot in state {@link TableState#FREE}
     */
    public static TableSnapshot provision(String tableId, String zone, int seats, Instant now) {
        return new TableSnapshot(tableId, zone, seats, TableState.FREE, now, 0L,
                null, null, false, 0, null, null, null, now);
    }

    /**
     * Dashboard colour token derived from {@link #state}. Serialised so that clients never have to
     * replicate the colour mapping.
     *
     * @return {@code GREEN}, {@code RED} or {@code AMBER}
     */
    @JsonProperty("colourName")
    public String colourName() {
        return state.colourName();
    }

    /**
     * Dashboard hex colour derived from {@link #state}.
     *
     * @return a six-digit hex colour prefixed with {@code #}
     */
    @JsonProperty("colourHex")
    public String colourHex() {
        return state.colourHex();
    }

    /**
     * Whether an SLA timer is currently armed for this table.
     *
     * @return {@code true} when both an SLA kind and a deadline are present
     */
    @JsonProperty("slaArmed")
    public boolean slaArmed() {
        return slaKind != null && slaDeadline != null;
    }

    /**
     * Remaining time before the active SLA deadline, floored at zero.
     *
     * @param now the reference instant; must not be {@code null}
     * @return remaining duration, or {@link Duration#ZERO} when no SLA is armed or it has elapsed
     */
    public Duration slaRemaining(Instant now) {
        Objects.requireNonNull(now, "now must not be null");
        if (slaDeadline == null) {
            return Duration.ZERO;
        }
        Duration remaining = Duration.between(now, slaDeadline);
        return remaining.isNegative() ? Duration.ZERO : remaining;
    }

    /**
     * How long the table has rested in its current state.
     *
     * @param now the reference instant; must not be {@code null}
     * @return the dwell duration, floored at zero
     */
    public Duration dwellTime(Instant now) {
        Objects.requireNonNull(now, "now must not be null");
        Duration dwell = Duration.between(stateSince, now);
        return dwell.isNegative() ? Duration.ZERO : dwell;
    }

    /**
     * Produces the next snapshot for an applied transition: state advanced, version incremented and
     * SLA fields rewritten wholesale so that no stale deadline can leak across a transition.
     *
     * @param nextState   the state being entered; must not be {@code null}
     * @param signal      the signal that caused the transition; must not be {@code null}
     * @param slaKind     the SLA to arm, or {@code null} to leave the table unguarded
     * @param slaDeadline the deadline to arm, or {@code null} to leave the table unguarded
     * @param now         the transition instant; must not be {@code null}
     * @return a new snapshot at {@code version + 1}
     */
    public TableSnapshot transitionedTo(TableState nextState,
                                        OccupancySignal signal,
                                        SlaKind slaKind,
                                        Instant slaDeadline,
                                        Instant now) {
        Objects.requireNonNull(nextState, "nextState must not be null");
        Objects.requireNonNull(signal, "signal must not be null");
        Objects.requireNonNull(now, "now must not be null");
        int nextOccupancy = nextState == TableState.SEATED ? signal.occupancy() : 0;
        return new TableSnapshot(tableId, zone, seats, nextState, now, version + 1L,
                slaKind, slaDeadline, false, nextOccupancy,
                signal.type(), signal.sourceKind(), lastWaiterAckAt, now);
    }

    /**
     * Produces the next snapshot for a waiter acknowledgement: the state is untouched, the SLA is
     * disarmed and the acknowledgement instant is recorded.
     *
     * @param signal the acknowledging signal; must not be {@code null}
     * @param now    the acknowledgement instant; must not be {@code null}
     * @return a new snapshot at {@code version + 1} with no armed SLA
     */
    public TableSnapshot acknowledged(OccupancySignal signal, Instant now) {
        Objects.requireNonNull(signal, "signal must not be null");
        Objects.requireNonNull(now, "now must not be null");
        return new TableSnapshot(tableId, zone, seats, state, stateSince, version + 1L,
                null, null, false, occupancy,
                signal.type(), signal.sourceKind(), now, now);
    }

    /**
     * Produces the next snapshot for an SLA breach: the state is untouched, the breach flag is
     * raised and the deadline is retained for forensic display on the dashboard.
     *
     * @param now the instant the breach was detected; must not be {@code null}
     * @return a new snapshot at {@code version + 1} with {@code slaBreached} raised
     */
    public TableSnapshot breached(Instant now) {
        Objects.requireNonNull(now, "now must not be null");
        return new TableSnapshot(tableId, zone, seats, state, stateSince, version + 1L,
                slaKind, slaDeadline, true, occupancy,
                lastSignalType, lastSignalSource, lastWaiterAckAt, now);
    }
}
