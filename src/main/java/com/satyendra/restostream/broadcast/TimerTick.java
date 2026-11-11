/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.broadcast;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.satyendra.restostream.domain.SlaKind;
import com.satyendra.restostream.domain.TableSnapshot;

import java.time.Instant;
import java.util.Objects;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>One table's live SLA countdown, broadcast on a fixed cadence so that every dashboard shows the
 * same remaining time regardless of its own clock skew or how long ago it connected.</p>
 *
 * <p>This is what satisfies the "broadcast state <em>and timer</em> changes" half of the live
 * broadcaster requirement. Without it a dashboard would have to count down locally from the
 * deadline in the last state message — which drifts, and which shows nothing at all to a dashboard
 * that connected after the table was seated.</p>
 *
 * @param tableId        the table being counted down
 * @param slaKind        the armed SLA class
 * @param deadline       the absolute breach instant
 * @param remainingMillis milliseconds remaining, floored at zero
 * @param breached       whether the SLA has already been breached and alerted
 * @param emittedAt      the instant this tick was produced
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TimerTick(
        String tableId,
        SlaKind slaKind,
        Instant deadline,
        long remainingMillis,
        boolean breached,
        Instant emittedAt) {

    /** Canonical constructor enforcing non-nullity of the mandatory components. */
    public TimerTick {
        Objects.requireNonNull(tableId, "tableId must not be null");
        Objects.requireNonNull(slaKind, "slaKind must not be null");
        Objects.requireNonNull(deadline, "deadline must not be null");
        Objects.requireNonNull(emittedAt, "emittedAt must not be null");
    }

    /**
     * Builds a tick from a table carrying an armed SLA.
     *
     * @param snapshot the table snapshot; must have an armed SLA
     * @param now      the reference instant; must not be {@code null}
     * @return the countdown tick
     * @throws IllegalArgumentException when the snapshot carries no armed SLA
     */
    public static TimerTick from(TableSnapshot snapshot, Instant now) {
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        Objects.requireNonNull(now, "now must not be null");
        if (!snapshot.slaArmed()) {
            throw new IllegalArgumentException(
                    "table " + snapshot.tableId() + " carries no armed SLA and cannot be ticked");
        }
        return new TimerTick(
                snapshot.tableId(),
                snapshot.slaKind(),
                snapshot.slaDeadline(),
                snapshot.slaRemaining(now).toMillis(),
                snapshot.slaBreached(),
                now);
    }
}
