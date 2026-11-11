/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.domain;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>The canonical lifecycle state of a physical restaurant table, together with the
 * dashboard colour semantics mandated by the RestoStream specification.</p>
 *
 * <p>The permitted transition graph is intentionally encoded on the enum itself rather than
 * scattered across services. This keeps the state machine a single source of truth that can be
 * reasoned about, unit tested in isolation and rendered as documentation:</p>
 *
 * <pre>
 *   FREE ──[GUEST_SEATED]──&gt; SEATED ──[GUEST_DEPARTED]──&gt; UNCLEANED ──[CLEANING_COMPLETED]──&gt; FREE
 *     └──────────────────────[CLEANING_REQUIRED]────────────&gt; UNCLEANED
 * </pre>
 *
 * <p>Every transition is guarded; no transition is implicit. Self-transitions are deliberately
 * <em>not</em> part of the graph — they are resolved one level up as idempotent no-ops, because a
 * repeated motion alert from a CCTV feed must never bump a table's version or re-arm its SLA.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public enum TableState {

    /** Table is clean, unoccupied and immediately sellable. Rendered green. */
    FREE("GREEN", "#2E7D32", false),

    /** Guests are seated at the table. Rendered red. Carries the waiter-response SLA. */
    SEATED("RED", "#C62828", true),

    /** Guests have left; the table awaits bussing before it can be sold again. Rendered amber. */
    UNCLEANED("AMBER", "#F9A825", true);

    private final String colourName;
    private final String colourHex;
    private final boolean slaTracked;

    TableState(String colourName, String colourHex, boolean slaTracked) {
        this.colourName = colourName;
        this.colourHex = colourHex;
        this.slaTracked = slaTracked;
    }

    /**
     * Human-readable colour token for the live dashboard legend.
     *
     * @return one of {@code GREEN}, {@code RED} or {@code AMBER}; never {@code null}
     */
    public String colourName() {
        return colourName;
    }

    /**
     * Hex colour used by the dashboard to paint the table tile.
     *
     * @return a six-digit hex colour prefixed with {@code #}; never {@code null}
     */
    public String colourHex() {
        return colourHex;
    }

    /**
     * Whether a table resting in this state is subject to a service-level agreement.
     *
     * @return {@code true} when entering this state should arm a distributed SLA timer
     */
    public boolean slaTracked() {
        return slaTracked;
    }

    /**
     * The service-level agreement that applies while a table rests in this state.
     *
     * @return the governing {@link SlaKind}, or {@code null} when this state is not SLA tracked
     */
    public SlaKind slaKind() {
        return switch (this) {
            case SEATED -> SlaKind.WAITER_RESPONSE;
            case UNCLEANED -> SlaKind.TABLE_TURNAROUND;
            case FREE -> null;
        };
    }

    /**
     * The set of states reachable from this state in a single legal transition.
     *
     * @return an immutable set of successor states; never {@code null}, possibly empty
     */
    public Set<TableState> allowedSuccessors() {
        return switch (this) {
            case FREE -> Collections.unmodifiableSet(EnumSet.of(SEATED, UNCLEANED));
            case SEATED -> Collections.unmodifiableSet(EnumSet.of(UNCLEANED));
            case UNCLEANED -> Collections.unmodifiableSet(EnumSet.of(FREE));
        };
    }

    /**
     * Guard predicate for the transition graph.
     *
     * @param target the candidate successor state; may be {@code null}
     * @return {@code true} when {@code this -> target} is a legal single-step transition
     */
    public boolean canTransitionTo(TableState target) {
        return target != null && allowedSuccessors().contains(target);
    }
}
