/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.engine;

import com.satyendra.restostream.domain.SlaKind;

import java.time.Duration;
import java.util.Optional;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Supplies the configured timeout for each class of service-level agreement. Expressed as a
 * narrow interface rather than injecting the configuration object directly, so that
 * {@link TableStateEngine} stays decoupled from Spring and trivially stubbable in unit tests.</p>
 *
 * <p>Returning {@link Optional#empty()} is a first-class answer meaning "this SLA is switched off".
 * The engine treats a disabled SLA as a cancellation rather than silently leaving a stale timer
 * armed from a previous state.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@FunctionalInterface
public interface SlaPolicy {

    /**
     * Resolves the timeout for the supplied SLA class.
     *
     * @param slaKind the SLA class to resolve; may be {@code null}, for which an empty result is
     *                returned so callers need not pre-check
     * @return the configured strictly-positive timeout, or empty when the SLA is disabled
     */
    Optional<Duration> timeoutFor(SlaKind slaKind);
}
