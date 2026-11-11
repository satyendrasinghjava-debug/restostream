/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.stomp;

import com.satyendra.restostream.domain.TableSnapshot;

import java.time.Instant;
import java.util.Objects;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>The answer to one dashboard command, delivered only to the session that issued it.</p>
 *
 * <p>STOMP has no request/response semantics, and the specification's own answer to a bad frame —
 * an {@code ERROR} frame — is terminal: it closes the connection. Tearing down a dashboard's live
 * feed because a waiter double-tapped a tile would be absurd, so business-level outcomes are
 * reported as an ordinary {@code MESSAGE} on a private destination instead, and {@code ERROR} is
 * reserved strictly for genuine protocol violations.</p>
 *
 * @param correlationId the value the caller supplied, echoed for client-side matching; may be {@code null}
 * @param tableId       the table the command concerned; may be {@code null} for a malformed command
 * @param status        the outcome
 * @param message       operator-facing explanation
 * @param snapshot      the authoritative table state after the command; {@code null} on failure
 * @param at            the instant the result was produced
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public record CommandResult(String correlationId,
                            String tableId,
                            Status status,
                            String message,
                            TableSnapshot snapshot,
                            Instant at) {

    /** Canonical constructor enforcing non-nullity of the mandatory components. */
    public CommandResult {
        Objects.requireNonNull(status, "status must not be null");
        Objects.requireNonNull(message, "message must not be null");
        Objects.requireNonNull(at, "at must not be null");
    }

    /**
     * Builds a successful result.
     *
     * @param command  the originating command
     * @param snapshot the resulting table state
     * @param applied  whether the state machine actually advanced
     * @param at       the instant the result was produced
     * @return the result
     */
    public static CommandResult accepted(TableCommand command,
                                         TableSnapshot snapshot,
                                         boolean applied,
                                         Instant at) {
        Status status = applied ? Status.APPLIED : Status.IGNORED;
        String message = applied
                ? "table %s is now %s".formatted(snapshot.tableId(), snapshot.state())
                : "table %s was already %s; nothing changed"
                        .formatted(snapshot.tableId(), snapshot.state());
        return new CommandResult(command.correlationId(), command.tableId(),
                status, message, snapshot, at);
    }

    /**
     * Builds a rejected result.
     *
     * @param correlationId the caller's correlation value; may be {@code null}
     * @param tableId       the table the command concerned; may be {@code null}
     * @param reason        operator-facing explanation
     * @param at            the instant the result was produced
     * @return the result
     */
    public static CommandResult rejected(String correlationId,
                                         String tableId,
                                         String reason,
                                         Instant at) {
        return new CommandResult(correlationId, tableId, Status.REJECTED, reason, null, at);
    }

    /**
     * Builds a failed result for an infrastructure or decoding failure.
     *
     * @param correlationId the caller's correlation value; may be {@code null}
     * @param tableId       the table the command concerned; may be {@code null}
     * @param reason        operator-facing explanation
     * @param at            the instant the result was produced
     * @return the result
     */
    public static CommandResult failed(String correlationId,
                                       String tableId,
                                       String reason,
                                       Instant at) {
        return new CommandResult(correlationId, tableId, Status.FAILED, reason, null, at);
    }

    /**
     * Outcomes a dashboard command can have.
     *
     * @author Satyendra Singh
     */
    public enum Status {

        /** The state machine advanced and the change has been broadcast. */
        APPLIED,

        /** The command asserted something already true; nothing changed. */
        IGNORED,

        /** The command contradicted the floor plan and was refused. */
        REJECTED,

        /** The command could not be processed at all — malformed, unknown table, or an outage. */
        FAILED
    }
}
