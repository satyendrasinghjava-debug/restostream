/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.alert;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.satyendra.restostream.domain.SlaKind;
import com.satyendra.restostream.domain.TableSnapshot;
import com.satyendra.restostream.domain.TableState;
import com.satyendra.restostream.timer.FiringSource;
import com.satyendra.restostream.timer.SlaTimerFiring;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>The asynchronous SLA alert message broadcast when a table's service-level agreement is
 * breached — the terminal output of the requirement "if no waiter event occurs within five minutes,
 * trigger an asynchronous SLA alert message".</p>
 *
 * <p>The alert is self-contained: it carries the table's identity, zone, state, both timestamps and
 * the overdue interval, so a consumer can route and render it without a follow-up read. It also
 * carries {@code detectedBy}, which lets operations distinguish a sub-second notification-driven
 * alert from one recovered by the reconciliation sweep.</p>
 *
 * @param alertId       unique identifier of this alert
 * @param tableId       the breaching table
 * @param zone          the table's dining zone
 * @param slaKind       the SLA that was breached
 * @param severity      operational severity
 * @param tableState    the state the table was resting in when the breach was confirmed
 * @param dueAt         the deadline that was missed
 * @param detectedAt    the instant the breach was confirmed
 * @param overdueMillis how late the breach was detected, in milliseconds
 * @param dwellMillis   how long the table had been in {@code tableState}, in milliseconds
 * @param occupancy     guest headcount last observed at the table
 * @param detectedBy    which detection path noticed the elapsed deadline
 * @param message       operator-facing message, ready to render in a toast
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SlaAlert(
        String alertId,
        String tableId,
        String zone,
        SlaKind slaKind,
        AlertSeverity severity,
        TableState tableState,
        Instant dueAt,
        Instant detectedAt,
        long overdueMillis,
        long dwellMillis,
        int occupancy,
        FiringSource detectedBy,
        String message) {

    /** Canonical constructor enforcing non-nullity of the mandatory components. */
    public SlaAlert {
        Objects.requireNonNull(alertId, "alertId must not be null");
        Objects.requireNonNull(tableId, "tableId must not be null");
        Objects.requireNonNull(slaKind, "slaKind must not be null");
        Objects.requireNonNull(severity, "severity must not be null");
        Objects.requireNonNull(tableState, "tableState must not be null");
        Objects.requireNonNull(detectedAt, "detectedAt must not be null");
        Objects.requireNonNull(detectedBy, "detectedBy must not be null");
        Objects.requireNonNull(message, "message must not be null");
    }

    /**
     * Builds an alert from the breached snapshot and the firing that detected it.
     *
     * @param breached the snapshot with its breach flag already raised; must not be {@code null}
     * @param firing   the claimed firing that triggered confirmation; must not be {@code null}
     * @return a fully-populated alert
     */
    public static SlaAlert from(TableSnapshot breached, SlaTimerFiring firing) {
        Objects.requireNonNull(breached, "breached must not be null");
        Objects.requireNonNull(firing, "firing must not be null");
        Instant dueAt = breached.slaDeadline();
        Instant detectedAt = firing.firedAt();
        long overdue = dueAt == null ? 0L : Math.max(0L, Duration.between(dueAt, detectedAt).toMillis());
        long dwell = breached.dwellTime(detectedAt).toMillis();
        AlertSeverity severity = AlertSeverity.forSla(firing.slaKind());
        return new SlaAlert(
                UUID.randomUUID().toString(),
                breached.tableId(),
                breached.zone(),
                firing.slaKind(),
                severity,
                breached.state(),
                dueAt,
                detectedAt,
                overdue,
                dwell,
                breached.occupancy(),
                firing.source(),
                describe(breached, firing, dwell));
    }

    private static String describe(TableSnapshot breached, SlaTimerFiring firing, long dwellMillis) {
        Duration dwell = Duration.ofMillis(dwellMillis);
        return switch (firing.slaKind()) {
            case WAITER_RESPONSE -> "Table %s in zone %s has been seated for %d min %02d s with no waiter response."
                    .formatted(breached.tableId(), breached.zone(),
                            dwell.toMinutes(), dwell.toSecondsPart());
            case TABLE_TURNAROUND -> "Table %s in zone %s has been awaiting cleaning for %d min %02d s."
                    .formatted(breached.tableId(), breached.zone(),
                            dwell.toMinutes(), dwell.toSecondsPart());
        };
    }
}
