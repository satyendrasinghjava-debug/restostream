/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.service;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Raised when a table's optimistic compare-and-set lost its race so many consecutive times that
 * the configured attempt budget was exhausted.</p>
 *
 * <p>This is a genuine back-pressure signal rather than a bug: it means one table is receiving
 * conflicting writes faster than Redis can serialise them. Surfaced as {@code 409 Conflict} with a
 * {@code Retry-After} hint so that a well-behaved client backs off instead of amplifying the
 * contention it is causing.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public class ConcurrencyExhaustedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String tableId;
    private final int attempts;

    /**
     * @param tableId  the contended table
     * @param attempts the number of attempts that were made
     */
    public ConcurrencyExhaustedException(String tableId, int attempts) {
        super("could not apply a transition to table '%s' after %d compare-and-set attempt(s); "
                .formatted(tableId, attempts)
                + "the table is under sustained concurrent modification");
        this.tableId = tableId;
        this.attempts = attempts;
    }

    /**
     * @return the contended table
     */
    public String tableId() {
        return tableId;
    }

    /**
     * @return the number of attempts that were made
     */
    public int attempts() {
        return attempts;
    }
}
