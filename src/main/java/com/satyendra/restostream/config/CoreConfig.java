/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.config;

import com.satyendra.restostream.engine.SlaPolicy;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Cross-cutting infrastructure beans.</p>
 *
 * <h2>Why the clock is a bean</h2>
 * <p>Nothing in RestoStream calls {@code Instant.now()}. Every timestamp — a state transition, an
 * SLA deadline, a breach detection, a countdown tick — is read from an injected {@link Clock}. That
 * single discipline is what makes a five-minute SLA testable in milliseconds: a test substitutes a
 * fixed or offset clock and the entire timer subsystem obeys it, with no {@code Thread.sleep} and no
 * flakiness. It also means a deployment can be pinned to UTC regardless of host configuration.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@Configuration(proxyBeanMethods = false)
public class CoreConfig {

    /**
     * The application clock, fixed to UTC so that persisted instants are unambiguous across
     * instances in different regions.
     *
     * @return a UTC system clock
     */
    @Bean
    @ConditionalOnMissingBean(Clock.class)
    public Clock applicationClock() {
        return Clock.systemUTC();
    }

    /**
     * Exposes the bound configuration as the narrow {@link SlaPolicy} the pure state engine depends
     * on, keeping the engine free of any knowledge of Spring or of the wider configuration tree.
     *
     * <p>A method reference is returned rather than the {@code properties} object itself, even
     * though {@link RestoStreamProperties} implements the interface directly. Returning the object
     * would publish a <em>second</em> bean of type {@code RestoStreamProperties} into the context —
     * one bound by {@code @ConfigurationPropertiesScan} and one by this factory method — and every
     * component that injects the configuration would then fail to start with an ambiguous
     * dependency. The reference adapts the type without duplicating the bean.</p>
     *
     * @param properties the bound configuration
     * @return an SLA policy view over that configuration
     */
    @Bean
    public SlaPolicy slaPolicy(RestoStreamProperties properties) {
        return properties::timeoutFor;
    }
}
