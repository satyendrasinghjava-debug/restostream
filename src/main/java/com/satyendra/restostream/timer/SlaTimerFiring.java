/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.timer;

import com.satyendra.restostream.domain.SlaKind;

import java.time.Instant;
import java.util.Objects;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>A claimed, de-duplicated notification that one table's SLA deadline has elapsed. Only the
 * instance that won the atomic claim in Redis ever constructs one of these, so a firing represents
 * a <em>single</em> unit of alerting work across the whole cluster.</p>
 *
 * @param tableId  the table whose deadline elapsed
 * @param slaKind  the SLA class that was armed
 * @param firedAt  the instant the elapse was detected
 * @param source   which detection path noticed it
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public record SlaTimerFiring(String tableId, SlaKind slaKind, Instant firedAt, FiringSource source) {

    /** Canonical constructor enforcing non-nullity of every component. */
    public SlaTimerFiring {
        Objects.requireNonNull(tableId, "tableId must not be null");
        Objects.requireNonNull(slaKind, "slaKind must not be null");
        Objects.requireNonNull(firedAt, "firedAt must not be null");
        Objects.requireNonNull(source, "source must not be null");
        if (tableId.isBlank()) {
            throw new IllegalArgumentException("tableId must not be blank");
        }
    }
}
