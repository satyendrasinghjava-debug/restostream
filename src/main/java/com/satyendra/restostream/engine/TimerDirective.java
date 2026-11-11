/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.engine;

import com.satyendra.restostream.domain.SlaKind;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>An immutable instruction describing what should happen to a table's distributed SLA timer as a
 * consequence of a state decision.</p>
 *
 * @param action   the side effect to perform
 * @param slaKind  the SLA being armed; {@code null} unless {@code action} is {@link TimerAction#ARM}
 * @param ttl      the timer lifetime; {@code null} unless {@code action} is {@link TimerAction#ARM}
 * @param deadline the absolute breach instant; {@code null} unless {@code action} is
 *                 {@link TimerAction#ARM}
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public record TimerDirective(TimerAction action, SlaKind slaKind, Duration ttl, Instant deadline) {

    private static final TimerDirective CANCEL = new TimerDirective(TimerAction.CANCEL, null, null, null);
    private static final TimerDirective NONE = new TimerDirective(TimerAction.NONE, null, null, null);

    /**
     * Canonical constructor enforcing that {@link TimerAction#ARM} always carries a complete,
     * strictly-positive timer specification, and that the other actions carry none at all.
     */
    public TimerDirective {
        Objects.requireNonNull(action, "action must not be null");
        if (action == TimerAction.ARM) {
            Objects.requireNonNull(slaKind, "slaKind must not be null for an ARM directive");
            Objects.requireNonNull(ttl, "ttl must not be null for an ARM directive");
            Objects.requireNonNull(deadline, "deadline must not be null for an ARM directive");
            if (ttl.isZero() || ttl.isNegative()) {
                throw new IllegalArgumentException("ttl must be strictly positive but was " + ttl);
            }
        } else if (slaKind != null || ttl != null || deadline != null) {
            throw new IllegalArgumentException(
                    "a " + action + " directive must not carry a timer specification");
        }
    }

    /**
     * Builds an {@link TimerAction#ARM} directive.
     *
     * @param slaKind  the SLA being armed; must not be {@code null}
     * @param ttl      strictly-positive timer lifetime
     * @param deadline absolute breach instant; must not be {@code null}
     * @return the directive
     */
    public static TimerDirective arm(SlaKind slaKind, Duration ttl, Instant deadline) {
        return new TimerDirective(TimerAction.ARM, slaKind, ttl, deadline);
    }

    /**
     * The shared {@link TimerAction#CANCEL} directive.
     *
     * @return a cancel directive
     */
    public static TimerDirective cancel() {
        return CANCEL;
    }

    /**
     * The shared {@link TimerAction#NONE} directive.
     *
     * @return a no-op directive
     */
    public static TimerDirective none() {
        return NONE;
    }

    /**
     * @return {@code true} when this directive arms a timer
     */
    public boolean isArm() {
        return action == TimerAction.ARM;
    }

    /**
     * @return {@code true} when this directive cancels a timer
     */
    public boolean isCancel() {
        return action == TimerAction.CANCEL;
    }
}
