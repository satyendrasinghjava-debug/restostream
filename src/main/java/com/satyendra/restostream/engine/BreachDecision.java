/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.engine;

import com.satyendra.restostream.domain.SlaKind;
import com.satyendra.restostream.domain.TableSnapshot;

import java.util.Objects;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>The closed result of asking {@link TableStateEngine} whether a fired SLA timer represents a
 * genuine breach.</p>
 *
 * <p>This re-validation step is not ceremony — it is what makes an at-least-once timer subsystem
 * safe. Redis key-expiry notifications are fire-and-forget, the reconciliation sweeper deliberately
 * re-examines overdue deadlines, and a waiter may have acknowledged the table in the milliseconds
 * between the key expiring and the event being delivered. Every one of those races resolves to
 * {@link Stale} here, so operators are never paged for a table that is already being served.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public sealed interface BreachDecision
        permits BreachDecision.Confirmed, BreachDecision.Stale {

    /**
     * Operator-facing explanation of the outcome.
     *
     * @return a non-blank narrative; never {@code null}
     */
    String narrative();

    /**
     * @return {@code true} only for {@link Confirmed}
     */
    default boolean isConfirmed() {
        return this instanceof Confirmed;
    }

    /**
     * The breach is real and has not been alerted before.
     *
     * @param current   the snapshot before the breach flag was raised
     * @param next      the snapshot with {@code slaBreached} raised, at {@code version + 1}
     * @param slaKind   the SLA that was breached
     * @param narrative operator-facing explanation
     */
    record Confirmed(TableSnapshot current,
                     TableSnapshot next,
                     SlaKind slaKind,
                     String narrative) implements BreachDecision {

        /** Canonical constructor enforcing non-nullity of every component. */
        public Confirmed {
            Objects.requireNonNull(current, "current must not be null");
            Objects.requireNonNull(next, "next must not be null");
            Objects.requireNonNull(slaKind, "slaKind must not be null");
            Objects.requireNonNull(narrative, "narrative must not be null");
        }
    }

    /**
     * The timer fired but the breach is no longer real — the table moved on, was acknowledged, was
     * already alerted, or the deadline has since been pushed out.
     *
     * @param current   the unchanged snapshot
     * @param narrative operator-facing explanation
     */
    record Stale(TableSnapshot current, String narrative) implements BreachDecision {

        /** Canonical constructor enforcing non-nullity of every component. */
        public Stale {
            Objects.requireNonNull(current, "current must not be null");
            Objects.requireNonNull(narrative, "narrative must not be null");
        }
    }
}
