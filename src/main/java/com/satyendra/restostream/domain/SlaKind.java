/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.domain;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Classifies the distributed timer that guards a table while it rests in an SLA-tracked state.
 * The kind is carried on both the Redis timer key and the emitted alert so that downstream
 * consumers (pager, dashboard toast, analytics sink) can route by SLA category without having to
 * re-derive it from the table state.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public enum SlaKind {

    /**
     * Guests are seated and nobody has attended the table yet. Breaching this SLA is the primary
     * operational signal the RestoStream specification is built around.
     */
    WAITER_RESPONSE("Waiter has not attended a seated table within the agreed window"),

    /**
     * The table has been vacated and is awaiting bussing. Breaching this SLA means revenue is
     * being lost to slow turnaround.
     */
    TABLE_TURNAROUND("Vacated table has not been cleaned within the agreed window");

    private final String description;

    SlaKind(String description) {
        this.description = description;
    }

    /**
     * Operator-facing explanation of what a breach of this SLA means.
     *
     * @return a non-blank human-readable description
     */
    public String description() {
        return description;
    }
}
