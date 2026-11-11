/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.alert;

import com.satyendra.restostream.domain.SlaKind;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Operational severity of an SLA breach, used by the dashboard to choose a toast style and by
 * downstream routing to decide whether to page a human.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public enum AlertSeverity {

    /** Revenue is being lost but no guest is currently being neglected. */
    WARNING,

    /** A guest is sitting unattended. This is the breach the specification is built to catch. */
    CRITICAL;

    /**
     * Maps an SLA class to its operational severity.
     *
     * @param slaKind the breached SLA class; must not be {@code null}
     * @return {@link #CRITICAL} for a guest-facing breach, otherwise {@link #WARNING}
     */
    public static AlertSeverity forSla(SlaKind slaKind) {
        return switch (slaKind) {
            case WAITER_RESPONSE -> CRITICAL;
            case TABLE_TURNAROUND -> WARNING;
        };
    }
}
