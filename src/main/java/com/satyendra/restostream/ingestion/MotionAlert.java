/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.ingestion;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.Objects;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>A single pixel/motion alert emitted by a CCTV edge device for one table's region of interest.
 * This is the wire format the future video pipeline will post — and the format the simulated
 * mobile-camera trigger uses today — so the two are interchangeable by construction.</p>
 *
 * <p>Note what is <em>not</em> here: no embeddings, no bounding boxes, no class probabilities, no
 * model identifier. The alert carries only cheap, classical computer-vision primitives that any
 * NVR, OpenCV background-subtractor or camera SoC can produce at frame rate on commodity hardware.
 * The occupancy judgement is then made by arithmetic in
 * {@link CctvMotionSignalSource} — the system is real-time precisely <em>because</em> no model sits
 * in the hot path.</p>
 *
 * @param alertId       unique identifier of this alert; doubles as the signal de-duplication key
 * @param cameraId      identifier of the emitting camera or NVR channel
 * @param tableId       the table whose region of interest produced the alert
 * @param zone          optional dining zone hint, may be {@code null}
 * @param capturedAt    the instant the frame was captured at the edge
 * @param motionRatio   fraction of the region of interest whose pixels changed, in {@code 0.0..1.0}
 * @param pixelDelta    absolute count of changed pixels, retained for diagnostics; never negative
 * @param occupancyHint cheap blob count used as a guest headcount hint, {@code 0} when unknown
 * @param confidence    edge confidence in the measurement, in {@code 0.0..1.0}
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record MotionAlert(
        String alertId,
        String cameraId,
        String tableId,
        String zone,
        Instant capturedAt,
        double motionRatio,
        long pixelDelta,
        int occupancyHint,
        double confidence) {

    /** Canonical constructor enforcing the alert's physical invariants. */
    public MotionAlert {
        Objects.requireNonNull(alertId, "alertId must not be null");
        Objects.requireNonNull(tableId, "tableId must not be null");
        Objects.requireNonNull(capturedAt, "capturedAt must not be null");
        if (tableId.isBlank()) {
            throw new IllegalArgumentException("tableId must not be blank");
        }
        if (motionRatio < 0.0d || motionRatio > 1.0d) {
            throw new IllegalArgumentException(
                    "motionRatio must be within [0.0, 1.0] but was " + motionRatio);
        }
        if (confidence < 0.0d || confidence > 1.0d) {
            throw new IllegalArgumentException(
                    "confidence must be within [0.0, 1.0] but was " + confidence);
        }
        if (pixelDelta < 0L) {
            throw new IllegalArgumentException("pixelDelta must not be negative but was " + pixelDelta);
        }
        if (occupancyHint < 0) {
            throw new IllegalArgumentException(
                    "occupancyHint must not be negative but was " + occupancyHint);
        }
        cameraId = cameraId == null || cameraId.isBlank() ? "unknown-camera" : cameraId;
    }
}
