/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.support;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>A {@link Clock} whose instant the test moves by hand.</p>
 *
 * <p>This is what lets a twenty-second debounce window, a five-minute SLA and a thirty-minute track
 * retention all be asserted inside the same millisecond of real time. Because every production
 * class takes its instants from an injected {@code Clock} rather than calling
 * {@code Instant.now()}, no test in this suite needs to sleep, and none of them is timing
 * sensitive.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public final class MutableClock extends Clock {

    private final ZoneId zone;
    private volatile Instant instant;

    /**
     * Creates a clock at the supplied instant in UTC.
     *
     * @param instant the starting instant; must not be {@code null}
     */
    public MutableClock(Instant instant) {
        this(instant, ZoneOffset.UTC);
    }

    private MutableClock(Instant instant, ZoneId zone) {
        this.instant = Objects.requireNonNull(instant, "instant must not be null");
        this.zone = Objects.requireNonNull(zone, "zone must not be null");
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId newZone) {
        return new MutableClock(instant, newZone);
    }

    @Override
    public Instant instant() {
        return instant;
    }

    /**
     * Moves the clock forward.
     *
     * @param amount how far to advance; must not be {@code null} or negative
     * @return this clock, for chaining
     */
    public MutableClock advance(Duration amount) {
        Objects.requireNonNull(amount, "amount must not be null");
        if (amount.isNegative()) {
            throw new IllegalArgumentException("a clock must not be moved backwards");
        }
        this.instant = this.instant.plus(amount);
        return this;
    }

    /**
     * Moves the clock to an absolute instant.
     *
     * @param to the instant to move to; must not be {@code null}
     * @return this clock, for chaining
     */
    public MutableClock set(Instant to) {
        this.instant = Objects.requireNonNull(to, "to must not be null");
        return this;
    }
}
