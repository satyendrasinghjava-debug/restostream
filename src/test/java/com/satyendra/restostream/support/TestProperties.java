/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.support;

import com.satyendra.restostream.config.RestoStreamProperties;

import java.time.Duration;
import java.util.List;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Builders for {@link RestoStreamProperties} in unit tests, so that no test has to spell out a
 * six-deep record tree in order to change one field.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public final class TestProperties {

    private TestProperties() {
        throw new AssertionError("TestProperties is a utility class and must not be instantiated");
    }

    /**
     * @return configuration equivalent to the shipped {@code application.yml} defaults
     */
    public static RestoStreamProperties defaults() {
        return new RestoStreamProperties(
                new RestoStreamProperties.Redis("restostream", 0),
                new RestoStreamProperties.Sla(Fixtures.WAITER_SLA, Fixtures.TURNAROUND_SLA),
                defaultTimers(),
                defaultIngestion(),
                defaultStomp(),
                new RestoStreamProperties.Layout(true, List.of(
                        new RestoStreamProperties.TableDefinition("T-01", "WINDOW", 4))));
    }

    /**
     * @return the default timer configuration
     */
    public static RestoStreamProperties.Timers defaultTimers() {
        return new RestoStreamProperties.Timers(
                true, "__keyevent@*__:expired", Duration.ofSeconds(1), 256,
                true, true, Duration.ofSeconds(5), true, Duration.ofSeconds(30));
    }

    /**
     * @return the default ingestion configuration
     */
    public static RestoStreamProperties.Ingestion defaultIngestion() {
        return new RestoStreamProperties.Ingestion(512, 16, Duration.ofMinutes(5),
                new RestoStreamProperties.Cctv(true, "cctv-motion", 0.18d, 0.04d, 3, 0.45d,
                        Duration.ofSeconds(20), Duration.ofMinutes(30)));
    }

    /**
     * @return the default STOMP configuration
     */
    public static RestoStreamProperties.Stomp defaultStomp() {
        return new RestoStreamProperties.Stomp("/ws/stomp", "/app", "/topic", 256,
                Duration.ofSeconds(10), Duration.ofSeconds(10), Duration.ofMinutes(5));
    }

    /**
     * Builds configuration whose STOMP section has been replaced.
     *
     * @param stomp the STOMP configuration to use
     * @return the configuration
     */
    public static RestoStreamProperties withStomp(RestoStreamProperties.Stomp stomp) {
        RestoStreamProperties base = defaults();
        return new RestoStreamProperties(base.redis(), base.sla(), base.timers(),
                base.ingestion(), stomp, base.layout());
    }

    /**
     * Builds configuration whose timer section has been replaced.
     *
     * @param timers the timer configuration to use
     * @return the configuration
     */
    public static RestoStreamProperties withTimers(RestoStreamProperties.Timers timers) {
        RestoStreamProperties base = defaults();
        return new RestoStreamProperties(base.redis(), base.sla(), timers,
                base.ingestion(), base.stomp(), base.layout());
    }

    /**
     * Builds configuration whose floor plan has been replaced.
     *
     * @param layout the layout to use
     * @return the configuration
     */
    public static RestoStreamProperties withLayout(RestoStreamProperties.Layout layout) {
        RestoStreamProperties base = defaults();
        return new RestoStreamProperties(base.redis(), base.sla(), base.timers(),
                base.ingestion(), base.stomp(), layout);
    }
}
