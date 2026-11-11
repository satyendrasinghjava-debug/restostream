/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.engine;

import com.satyendra.restostream.domain.OccupancySignal;
import com.satyendra.restostream.domain.TableSnapshot;

import java.util.Objects;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>The total, closed result of asking {@link TableStateEngine} to evaluate one signal against one
 * table. Modelled as a sealed hierarchy so that every call site is forced by the compiler to handle
 * all three outcomes — there is no way to accidentally treat a rejected transition as applied.</p>
 *
 * <p>The three outcomes are deliberately distinct rather than collapsed into a boolean:</p>
 * <ul>
 *   <li>{@link Applied} — the state machine advanced; persist, re-arm the timer and broadcast.</li>
 *   <li>{@link Ignored} — the signal was a duplicate of reality; do nothing at all. This is the
 *       common case for CCTV motion feeds, which re-assert the same truth many times a minute.</li>
 *   <li>{@link Rejected} — the signal contradicted the floor plan; surface a conflict to the
 *       operator and leave state untouched.</li>
 * </ul>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public sealed interface TransitionDecision
        permits TransitionDecision.Applied,
                TransitionDecision.Ignored,
                TransitionDecision.Rejected {

    /**
     * The table state as it stands <em>before</em> this decision is executed.
     *
     * @return the pre-decision snapshot; never {@code null}
     */
    TableSnapshot current();

    /**
     * The signal that was evaluated.
     *
     * @return the evaluated signal; never {@code null}
     */
    OccupancySignal signal();

    /**
     * Operator-facing explanation of the outcome, suitable for an audit log line.
     *
     * @return a non-blank narrative; never {@code null}
     */
    String narrative();

    /**
     * @return {@code true} only for {@link Applied}
     */
    default boolean isApplied() {
        return this instanceof Applied;
    }

    /**
     * The state machine advanced.
     *
     * @param current  the snapshot before the transition
     * @param next     the snapshot after the transition, already at {@code version + 1}
     * @param signal   the signal that caused the transition
     * @param timer    the timer side effect the orchestration layer must execute
     * @param narrative operator-facing explanation
     */
    record Applied(TableSnapshot current,
                   TableSnapshot next,
                   OccupancySignal signal,
                   TimerDirective timer,
                   String narrative) implements TransitionDecision {

        /** Canonical constructor enforcing non-nullity of every component. */
        public Applied {
            Objects.requireNonNull(current, "current must not be null");
            Objects.requireNonNull(next, "next must not be null");
            Objects.requireNonNull(signal, "signal must not be null");
            Objects.requireNonNull(timer, "timer must not be null");
            Objects.requireNonNull(narrative, "narrative must not be null");
        }
    }

    /**
     * The signal asserted something already true. No state, version, timer or broadcast changes.
     *
     * @param current   the unchanged snapshot
     * @param signal    the duplicate signal
     * @param narrative operator-facing explanation
     */
    record Ignored(TableSnapshot current,
                   OccupancySignal signal,
                   String narrative) implements TransitionDecision {

        /** Canonical constructor enforcing non-nullity of every component. */
        public Ignored {
            Objects.requireNonNull(current, "current must not be null");
            Objects.requireNonNull(signal, "signal must not be null");
            Objects.requireNonNull(narrative, "narrative must not be null");
        }
    }

    /**
     * The signal contradicted the floor plan and was refused.
     *
     * @param current   the unchanged snapshot
     * @param signal    the refused signal
     * @param narrative operator-facing explanation
     */
    record Rejected(TableSnapshot current,
                    OccupancySignal signal,
                    String narrative) implements TransitionDecision {

        /** Canonical constructor enforcing non-nullity of every component. */
        public Rejected {
            Objects.requireNonNull(current, "current must not be null");
            Objects.requireNonNull(signal, "signal must not be null");
            Objects.requireNonNull(narrative, "narrative must not be null");
        }

        /**
         * Converts this refusal into the exception the REST boundary surfaces as {@code 409}.
         *
         * @return a fully-populated {@link InvalidTransitionException}
         */
        public InvalidTransitionException toException() {
            return new InvalidTransitionException(
                    current.tableId(), current.state(), signal.type(), narrative);
        }
    }
}
