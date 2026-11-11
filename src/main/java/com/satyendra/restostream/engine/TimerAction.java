/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.engine;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>The side effect the state engine requests of the distributed timer subsystem. The engine never
 * touches Redis itself — it returns an intent, and the orchestration layer executes it. That
 * separation is what makes the engine a pure function and therefore exhaustively unit testable
 * without a single mock of a network client.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public enum TimerAction {

    /** Arm (or re-arm, replacing any existing timer) an SLA timer for the table. */
    ARM,

    /** Cancel any SLA timer currently armed for the table. */
    CANCEL,

    /** Leave the timer subsystem untouched. */
    NONE
}
