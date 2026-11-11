/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.web.dto;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>A single, predictable error shape for every non-2xx response the API produces, so that a
 * dashboard has exactly one failure branch to render rather than one per framework default.</p>
 *
 * @param status    the HTTP status code
 * @param error     the short, machine-readable error code
 * @param message   the operator-facing explanation
 * @param path      the request path
 * @param details   field-level or contextual details; never {@code null}, possibly empty
 * @param timestamp the instant the error was produced
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public record ApiError(int status,
                       String error,
                       String message,
                       String path,
                       List<String> details,
                       Instant timestamp) {

    /** Canonical constructor enforcing non-nullity and normalising the detail list. */
    public ApiError {
        Objects.requireNonNull(error, "error must not be null");
        Objects.requireNonNull(message, "message must not be null");
        Objects.requireNonNull(timestamp, "timestamp must not be null");
        details = details == null ? List.of() : List.copyOf(details);
    }

    /**
     * Builds an error without field-level details.
     *
     * @param status    the HTTP status code
     * @param error     the short error code
     * @param message   the explanation
     * @param path      the request path
     * @param timestamp the instant the error was produced
     * @return the error body
     */
    public static ApiError of(int status, String error, String message, String path, Instant timestamp) {
        return new ApiError(status, error, message, path, List.of(), timestamp);
    }
}
