/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.web.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.satyendra.restostream.domain.SignalSourceKind;
import com.satyendra.restostream.domain.SignalType;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Request body for raising an occupancy signal against a table over HTTP. The table identifier
 * comes from the path, so the body carries only the assertion itself.</p>
 *
 * @param signal    what is being asserted; required
 * @param occupancy guest headcount, when known; may be {@code null}
 * @param actor     identifier of the person or device raising the signal; may be {@code null}
 * @param source    transport classification to record; may be {@code null} to default to a manual tap
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record SignalRequest(
        @NotNull(message = "signal is required and must be one of GUEST_SEATED, GUEST_DEPARTED, "
                + "WAITER_ATTENDED, CLEANING_COMPLETED, CLEANING_REQUIRED")
        SignalType signal,

        @Min(value = 0, message = "occupancy must not be negative")
        @Max(value = 64, message = "occupancy must not exceed 64")
        Integer occupancy,

        String actor,

        SignalSourceKind source) {

    /**
     * @return the transport classification to record, defaulting to a manual tap
     */
    public SignalSourceKind effectiveSource() {
        return source == null ? SignalSourceKind.MANUAL_TAP : source;
    }

    /**
     * @return the actor identifier to record, defaulting to an anonymous API caller
     */
    public String effectiveActor() {
        return actor == null || actor.isBlank() ? "rest-api" : actor;
    }

    /**
     * @return the guest headcount to record, defaulting to zero when unknown
     */
    public int effectiveOccupancy() {
        return occupancy == null || occupancy < 0 ? 0 : occupancy;
    }
}
