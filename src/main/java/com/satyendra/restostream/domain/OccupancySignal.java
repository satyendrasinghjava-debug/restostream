/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>An immutable, transport-agnostic occupancy signal — the single unit of input to RestoStream.
 * Whether it originated from a manual dashboard tap, a waiter's phone camera or a CCTV pixel-delta
 * alert, by the time it reaches the ingestion gateway it is this record and nothing else.</p>
 *
 * <p>The record carries an idempotency-friendly {@code signalId} and an {@code observedAt}
 * timestamp taken at the edge. {@code observedAt} is the physical-world instant; it is distinct
 * from the instant the middleware processed the signal, which matters because a CCTV batch may
 * arrive seconds late and must not be allowed to silently re-date a table's SLA deadline.</p>
 *
 * @param signalId    unique identifier of this observation, used for de-duplication
 * @param tableId     the table the observation concerns
 * @param type        what the physical world is asserting
 * @param sourceKind  the transport that produced the observation
 * @param sourceId    identifier of the concrete producer instance (camera id, device id, user id)
 * @param observedAt  the instant the observation was made at the edge
 * @param confidence  edge confidence in {@code 0.0..1.0}; manual taps are {@code 1.0}
 * @param occupancy   number of guests observed, or {@code 0} when not applicable/unknown
 * @param attributes  free-form diagnostic attributes preserved for audit; never {@code null}
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record OccupancySignal(
        String signalId,
        String tableId,
        SignalType type,
        SignalSourceKind sourceKind,
        String sourceId,
        Instant observedAt,
        double confidence,
        int occupancy,
        Map<String, String> attributes) {

    /**
     * Canonical constructor enforcing the record's invariants. Signals are the system's trust
     * boundary, so validation lives here rather than in each transport.
     */
    public OccupancySignal {
        Objects.requireNonNull(signalId, "signalId must not be null");
        Objects.requireNonNull(tableId, "tableId must not be null");
        Objects.requireNonNull(type, "type must not be null");
        Objects.requireNonNull(sourceKind, "sourceKind must not be null");
        Objects.requireNonNull(observedAt, "observedAt must not be null");
        if (tableId.isBlank()) {
            throw new IllegalArgumentException("tableId must not be blank");
        }
        if (confidence < 0.0d || confidence > 1.0d) {
            throw new IllegalArgumentException(
                    "confidence must be within [0.0, 1.0] but was " + confidence);
        }
        if (occupancy < 0) {
            throw new IllegalArgumentException("occupancy must not be negative but was " + occupancy);
        }
        sourceId = sourceId == null ? sourceKind.name().toLowerCase(java.util.Locale.ROOT) : sourceId;
        attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
    }

    /**
     * Factory for a fully-trusted signal raised by a human action.
     *
     * @param tableId    the table the observation concerns
     * @param type       what is being asserted
     * @param sourceKind the transport that produced the observation
     * @param sourceId   identifier of the concrete producer instance
     * @param observedAt the instant the observation was made
     * @return a signal with confidence {@code 1.0} and a generated identifier
     */
    public static OccupancySignal trusted(String tableId,
                                          SignalType type,
                                          SignalSourceKind sourceKind,
                                          String sourceId,
                                          Instant observedAt) {
        return new OccupancySignal(
                UUID.randomUUID().toString(), tableId, type, sourceKind,
                sourceId, observedAt, 1.0d, 0, Map.of());
    }

    /**
     * Returns a copy of this signal with the supplied occupancy headcount.
     *
     * @param newOccupancy the guest headcount to record; must not be negative
     * @return a new signal instance; this instance is left untouched
     */
    public OccupancySignal withOccupancy(int newOccupancy) {
        return new OccupancySignal(signalId, tableId, type, sourceKind, sourceId,
                observedAt, confidence, newOccupancy, attributes);
    }

    /**
     * Returns a copy of this signal carrying an additional diagnostic attribute.
     *
     * @param key   attribute name; must not be {@code null}
     * @param value attribute value; must not be {@code null}
     * @return a new signal instance; this instance is left untouched
     */
    public OccupancySignal withAttribute(String key, String value) {
        Objects.requireNonNull(key, "attribute key must not be null");
        Objects.requireNonNull(value, "attribute value must not be null");
        Map<String, String> merged = new java.util.LinkedHashMap<>(attributes);
        merged.put(key, value);
        return new OccupancySignal(signalId, tableId, type, sourceKind, sourceId,
                observedAt, confidence, occupancy, merged);
    }
}
