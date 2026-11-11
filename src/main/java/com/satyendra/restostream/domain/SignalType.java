/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.domain;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>The vocabulary of occupancy signals that RestoStream ingests. A signal is an <em>intent</em>
 * expressed by the physical world; it is deliberately decoupled from {@link TableState} so that
 * additional transports (manual tap, mobile camera, CCTV motion, POS ticket close) can all speak
 * the same language without knowing anything about the state machine.</p>
 *
 * <p>Each signal declares the state it drives a table towards. {@link #WAITER_ATTENDED} is the one
 * signal that drives no transition at all: it exists purely to acknowledge service and thereby
 * disarm the waiter-response SLA timer.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public enum SignalType {

    /** Guests have sat down. Drives the table to {@link TableState#SEATED}. */
    GUEST_SEATED(TableState.SEATED),

    /** Guests have left. Drives the table to {@link TableState#UNCLEANED}. */
    GUEST_DEPARTED(TableState.UNCLEANED),

    /** A waiter has attended a seated table. Drives no transition; disarms the SLA timer. */
    WAITER_ATTENDED(null),

    /** Bussing is finished. Drives the table to {@link TableState#FREE}. */
    CLEANING_COMPLETED(TableState.FREE),

    /** A clean, unoccupied table has been soiled and needs attention before it can be sold. */
    CLEANING_REQUIRED(TableState.UNCLEANED);

    private final TableState targetState;

    SignalType(TableState targetState) {
        this.targetState = targetState;
    }

    /**
     * The state this signal drives a table towards.
     *
     * @return the target state, or {@link Optional#empty()} for acknowledgement-only signals
     */
    public Optional<TableState> targetState() {
        return Optional.ofNullable(targetState);
    }

    /**
     * Whether this signal is a pure service acknowledgement rather than a transition request.
     *
     * @return {@code true} when the signal carries no target state
     */
    public boolean isAcknowledgement() {
        return targetState == null;
    }

    /**
     * The states from which this signal is operationally meaningful.
     *
     * <p>This is a distinct, stricter guard from {@link TableState#allowedSuccessors()}. The state
     * graph expresses the <em>structural</em> invariant (which states are adjacent); this set
     * expresses <em>operational policy</em> (which signal may legitimately drive that edge). Both
     * {@code GUEST_DEPARTED} and {@code CLEANING_REQUIRED} target {@link TableState#UNCLEANED}, but
     * only the former may be raised against a table that currently has guests at it — otherwise a
     * misrouted housekeeping alert would evict paying customers from the floor plan.</p>
     *
     * @return an immutable, non-empty set of legal origin states
     */
    public Set<TableState> permittedOriginStates() {
        return switch (this) {
            case GUEST_SEATED -> Collections.unmodifiableSet(EnumSet.of(TableState.FREE));
            case GUEST_DEPARTED -> Collections.unmodifiableSet(EnumSet.of(TableState.SEATED));
            case WAITER_ATTENDED -> Collections.unmodifiableSet(EnumSet.of(TableState.SEATED));
            case CLEANING_COMPLETED -> Collections.unmodifiableSet(EnumSet.of(TableState.UNCLEANED));
            case CLEANING_REQUIRED -> Collections.unmodifiableSet(EnumSet.of(TableState.FREE));
        };
    }

    /**
     * Operational-policy guard.
     *
     * @param origin the state the table is currently resting in; may be {@code null}
     * @return {@code true} when this signal may legitimately be raised from {@code origin}
     */
    public boolean isPermittedFrom(TableState origin) {
        return origin != null && permittedOriginStates().contains(origin);
    }
}
