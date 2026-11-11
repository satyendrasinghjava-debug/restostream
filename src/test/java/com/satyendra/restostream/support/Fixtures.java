/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.support;

import com.satyendra.restostream.domain.OccupancySignal;
import com.satyendra.restostream.domain.SignalSourceKind;
import com.satyendra.restostream.domain.SignalType;
import com.satyendra.restostream.domain.SlaKind;
import com.satyendra.restostream.domain.TableSnapshot;
import com.satyendra.restostream.domain.TableState;
import com.satyendra.restostream.engine.SlaPolicy;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Shared, deterministic test data. Every fixture is built from an explicit {@link Instant} so
 * that no test depends on wall-clock time — which is what allows a five-minute SLA to be asserted
 * in microseconds with no sleeping and no flakiness.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public final class Fixtures {

    /** Canonical table identifier used throughout the unit suite. */
    public static final String TABLE_ID = "T-01";

    /** Canonical dining zone. */
    public static final String ZONE = "WINDOW";

    /** A fixed reference instant, chosen to be unambiguous in any time zone. */
    public static final Instant T0 = Instant.parse("2026-11-08T18:30:00Z");

    /** The specification's five-minute waiter-response SLA. */
    public static final Duration WAITER_SLA = Duration.ofMinutes(5);

    /** The configured table-turnaround SLA. */
    public static final Duration TURNAROUND_SLA = Duration.ofMinutes(3);

    private Fixtures() {
        throw new AssertionError("Fixtures is a utility class and must not be instantiated");
    }

    /**
     * @param at the instant to freeze at
     * @return a UTC clock frozen at {@code at}
     */
    public static Clock clockAt(Instant at) {
        return Clock.fixed(at, ZoneOffset.UTC);
    }

    /**
     * @return an SLA policy with both SLAs configured at their specification values
     */
    public static SlaPolicy defaultPolicy() {
        return policy(WAITER_SLA, TURNAROUND_SLA);
    }

    /**
     * Builds an SLA policy. A {@code null} duration means that SLA is switched off.
     *
     * @param waiterResponse  the waiter-response timeout, or {@code null} to disable
     * @param tableTurnaround the table-turnaround timeout, or {@code null} to disable
     * @return the policy
     */
    public static SlaPolicy policy(Duration waiterResponse, Duration tableTurnaround) {
        return slaKind -> {
            if (slaKind == null) {
                return Optional.empty();
            }
            Duration configured = slaKind == SlaKind.WAITER_RESPONSE ? waiterResponse : tableTurnaround;
            return Optional.ofNullable(configured);
        };
    }

    /**
     * @return a freshly provisioned, version-zero {@code FREE} table at {@link #T0}
     */
    public static TableSnapshot freeTable() {
        return TableSnapshot.provision(TABLE_ID, ZONE, 4, T0);
    }

    /**
     * Builds a {@code SEATED} table carrying an armed waiter-response SLA.
     *
     * @param seatedAt when the guests sat down
     * @param sla      the SLA window; the deadline becomes {@code seatedAt + sla}
     * @return the snapshot, at version 1
     */
    public static TableSnapshot seatedTable(Instant seatedAt, Duration sla) {
        return new TableSnapshot(TABLE_ID, ZONE, 4, TableState.SEATED, seatedAt, 1L,
                SlaKind.WAITER_RESPONSE, seatedAt.plus(sla), false, 2,
                SignalType.GUEST_SEATED, SignalSourceKind.MANUAL_TAP, null, seatedAt);
    }

    /**
     * Builds a {@code SEATED} table with no armed SLA, as it is after a waiter acknowledgement.
     *
     * @param seatedAt     when the guests sat down
     * @param acknowledged when the waiter attended
     * @return the snapshot, at version 2
     */
    public static TableSnapshot acknowledgedTable(Instant seatedAt, Instant acknowledged) {
        return new TableSnapshot(TABLE_ID, ZONE, 4, TableState.SEATED, seatedAt, 2L,
                null, null, false, 2,
                SignalType.WAITER_ATTENDED, SignalSourceKind.MANUAL_TAP, acknowledged, acknowledged);
    }

    /**
     * Builds an {@code UNCLEANED} table carrying an armed turnaround SLA.
     *
     * @param vacatedAt when the guests left
     * @param sla       the SLA window
     * @return the snapshot, at version 2
     */
    public static TableSnapshot uncleanedTable(Instant vacatedAt, Duration sla) {
        return new TableSnapshot(TABLE_ID, ZONE, 4, TableState.UNCLEANED, vacatedAt, 2L,
                SlaKind.TABLE_TURNAROUND, vacatedAt.plus(sla), false, 0,
                SignalType.GUEST_DEPARTED, SignalSourceKind.MANUAL_TAP, null, vacatedAt);
    }

    /**
     * Builds a table resting in an arbitrary state with no armed SLA.
     *
     * @param state the state to rest in
     * @param at    the instant it entered that state
     * @return the snapshot, at version 3
     */
    public static TableSnapshot tableIn(TableState state, Instant at) {
        return new TableSnapshot(TABLE_ID, ZONE, 4, state, at, 3L,
                null, null, false, state == TableState.SEATED ? 2 : 0,
                null, null, null, at);
    }

    /**
     * Builds a signal for the canonical table.
     *
     * @param type what is being asserted
     * @param at   the observation instant
     * @return the signal, with confidence 1.0 and no headcount
     */
    public static OccupancySignal signal(SignalType type, Instant at) {
        return signal(TABLE_ID, type, at, 0);
    }

    /**
     * Builds a signal.
     *
     * @param tableId   the table being asserted about
     * @param type      what is being asserted
     * @param at        the observation instant
     * @param occupancy the guest headcount
     * @return the signal
     */
    public static OccupancySignal signal(String tableId, SignalType type, Instant at, int occupancy) {
        return new OccupancySignal("sig-" + type + '-' + at.toEpochMilli(), tableId, type,
                SignalSourceKind.MANUAL_TAP, "unit-test", at, 1.0d, occupancy,
                Map.of("transport", "unit-test"));
    }
}
