/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.broadcast;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Classifies a broadcast payload. Sent as a STOMP {@code content-kind} header and as the
 * Server-Sent Events {@code event} name, so a dashboard can dispatch on it without sniffing the
 * JSON body.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public enum BroadcastKind {

    /** A single {@link com.satyendra.restostream.domain.TableSnapshot}. */
    TABLE_STATE,

    /** The complete floor plan, sent when a dashboard first subscribes. */
    FLOOR_SNAPSHOT,

    /** An {@link com.satyendra.restostream.alert.SlaAlert}. */
    SLA_ALERT,

    /** A {@link TimerTick} batch carrying live SLA countdowns. */
    TIMER_TICK
}
