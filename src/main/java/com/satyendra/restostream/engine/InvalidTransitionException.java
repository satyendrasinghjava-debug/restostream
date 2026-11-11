/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.engine;

import com.satyendra.restostream.domain.SignalType;
import com.satyendra.restostream.domain.TableState;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Raised when a caller insists on applying a signal the state machine has refused. The engine
 * itself never throws this — it returns a {@link TransitionDecision.Rejected} value. The exception
 * exists for the imperative boundary (the REST layer), which must surface {@code 409 Conflict} to
 * the operator rather than silently swallowing an illegal floor-plan operation.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public class InvalidTransitionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final String tableId;
    private final TableState currentState;
    private final SignalType signalType;

    /**
     * Creates a new exception describing a refused transition.
     *
     * @param tableId      the table the signal was raised against
     * @param currentState the state the table is actually resting in
     * @param signalType   the signal that was refused
     * @param reason       operator-facing explanation
     */
    public InvalidTransitionException(String tableId,
                                      TableState currentState,
                                      SignalType signalType,
                                      String reason) {
        super(reason);
        this.tableId = tableId;
        this.currentState = currentState;
        this.signalType = signalType;
    }

    /**
     * @return the table the refused signal was raised against
     */
    public String tableId() {
        return tableId;
    }

    /**
     * @return the state the table is actually resting in
     */
    public TableState currentState() {
        return currentState;
    }

    /**
     * @return the signal that was refused
     */
    public SignalType signalType() {
        return signalType;
    }
}
