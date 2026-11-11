/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.stomp;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.satyendra.restostream.domain.SignalSourceKind;
import com.satyendra.restostream.domain.SignalType;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>The JSON body a dashboard sends in a STOMP {@code SEND} frame addressed to
 * {@code /app/tables/signal} — the "manual tap" trigger. Every field but {@code tableId} and
 * {@code signal} is optional, because a waiter tapping a tile has nothing else to offer.</p>
 *
 * @param tableId       the table being asserted about
 * @param signal        what is being asserted
 * @param occupancy     guest headcount, when known; {@code null} otherwise
 * @param actor         identifier of the person or device issuing the command; {@code null} to default
 * @param source        the transport classification to record; {@code null} defaults to a manual tap
 * @param correlationId caller-supplied value echoed back on the command result; {@code null} to omit
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TableCommand(String tableId,
                           SignalType signal,
                           Integer occupancy,
                           String actor,
                           SignalSourceKind source,
                           String correlationId) {

    /**
     * @return the transport classification to record, defaulting to a manual tap
     */
    public SignalSourceKind effectiveSource() {
        return source == null ? SignalSourceKind.MANUAL_TAP : source;
    }

    /**
     * @return the actor identifier to record, defaulting to an anonymous dashboard
     */
    public String effectiveActor() {
        return actor == null || actor.isBlank() ? "dashboard" : actor;
    }

    /**
     * @return the guest headcount to record, defaulting to zero when unknown
     */
    public int effectiveOccupancy() {
        return occupancy == null || occupancy < 0 ? 0 : occupancy;
    }
}
