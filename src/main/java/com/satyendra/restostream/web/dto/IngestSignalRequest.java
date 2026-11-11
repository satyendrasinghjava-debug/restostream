/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.web.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.satyendra.restostream.domain.OccupancySignal;
import com.satyendra.restostream.domain.SignalSourceKind;
import com.satyendra.restostream.domain.SignalType;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Request body for the fire-and-forget ingestion endpoint, where the table identifier travels in
 * the body rather than the path because a transport may batch observations across tables.</p>
 *
 * @param signalId   caller-supplied de-duplication key; generated when omitted
 * @param tableId    the table being asserted about; required
 * @param signal     what is being asserted; required
 * @param occupancy  guest headcount, when known
 * @param actor      identifier of the person or device
 * @param source     transport classification; defaults to a mobile camera, the simulated trigger
 * @param observedAt edge observation instant; defaults to the arrival instant
 * @param confidence edge confidence, {@code 0.0..1.0}; defaults to {@code 1.0}
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record IngestSignalRequest(
        String signalId,

        @NotBlank(message = "tableId is required")
        String tableId,

        @NotNull(message = "signal is required")
        SignalType signal,

        @Min(value = 0, message = "occupancy must not be negative")
        @Max(value = 64, message = "occupancy must not exceed 64")
        Integer occupancy,

        String actor,

        SignalSourceKind source,

        Instant observedAt,

        @DecimalMin(value = "0.0", message = "confidence must not be below 0.0")
        @DecimalMax(value = "1.0", message = "confidence must not exceed 1.0")
        Double confidence) {

    /**
     * Converts this request into the immutable domain signal, filling in server-side defaults.
     *
     * @param clock the application clock used for the arrival instant; must not be {@code null}
     * @return the domain signal
     */
    public OccupancySignal toDomain(Clock clock) {
        Instant arrival = clock.instant();
        SignalSourceKind kind = source == null ? SignalSourceKind.MOBILE_CAMERA : source;
        return new OccupancySignal(
                signalId == null || signalId.isBlank() ? UUID.randomUUID().toString() : signalId,
                tableId,
                signal,
                kind,
                actor == null || actor.isBlank() ? kind.name().toLowerCase(java.util.Locale.ROOT) : actor,
                observedAt == null ? arrival : observedAt,
                confidence == null ? 1.0d : confidence,
                occupancy == null || occupancy < 0 ? 0 : occupancy,
                Map.of("transport", "rest-ingest"));
    }
}
