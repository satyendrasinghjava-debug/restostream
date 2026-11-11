/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.web.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.satyendra.restostream.ingestion.MotionAlert;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Request body for a CCTV pixel/motion alert. This is the contract a video pipeline will post
 * against; the only difference between today's simulated trigger and tomorrow's NVR is which
 * process fills in the numbers.</p>
 *
 * <p>Optional fields are generated server-side so that a constrained edge device can post the
 * smallest possible payload: {@code alertId} becomes a UUID for de-duplication and
 * {@code capturedAt} becomes the arrival instant.</p>
 *
 * @param alertId       unique alert identifier; generated when omitted
 * @param cameraId      emitting camera or NVR channel; defaults server-side when omitted
 * @param tableId       the table whose region of interest produced the alert; required
 * @param zone          dining zone hint; may be {@code null}
 * @param capturedAt    edge capture instant; defaults to the arrival instant when omitted
 * @param motionRatio   fraction of the region of interest whose pixels changed, {@code 0.0..1.0}
 * @param pixelDelta    absolute changed-pixel count, for diagnostics
 * @param occupancyHint cheap blob count used as a headcount hint
 * @param confidence    edge confidence, {@code 0.0..1.0}; defaults to {@code 1.0} when omitted
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record MotionAlertRequest(
        String alertId,

        String cameraId,

        @NotBlank(message = "tableId is required")
        String tableId,

        String zone,

        Instant capturedAt,

        @DecimalMin(value = "0.0", message = "motionRatio must not be below 0.0")
        @DecimalMax(value = "1.0", message = "motionRatio must not exceed 1.0")
        double motionRatio,

        @Min(value = 0, message = "pixelDelta must not be negative")
        long pixelDelta,

        @Min(value = 0, message = "occupancyHint must not be negative")
        @Max(value = 64, message = "occupancyHint must not exceed 64")
        int occupancyHint,

        @DecimalMin(value = "0.0", message = "confidence must not be below 0.0")
        @DecimalMax(value = "1.0", message = "confidence must not exceed 1.0")
        Double confidence) {

    /**
     * Converts this request into the immutable domain alert, filling in server-side defaults.
     *
     * @param clock the application clock used for the arrival instant; must not be {@code null}
     * @return the domain alert
     */
    public MotionAlert toDomain(Clock clock) {
        Instant arrival = clock.instant();
        return new MotionAlert(
                alertId == null || alertId.isBlank() ? UUID.randomUUID().toString() : alertId,
                cameraId,
                tableId,
                zone,
                capturedAt == null ? arrival : capturedAt,
                motionRatio,
                pixelDelta,
                occupancyHint,
                confidence == null ? 1.0d : confidence);
    }
}
