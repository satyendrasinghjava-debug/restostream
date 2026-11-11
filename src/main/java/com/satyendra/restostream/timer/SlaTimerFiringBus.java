/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.Objects;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>In-process fan-in point for claimed SLA firings. Both detection paths —
 * {@link SlaExpiryListener} and {@link SlaReconciliationSweeper} — publish here, and the alert
 * dispatcher consumes from here.</p>
 *
 * <p>The indirection buys genuine decoupling: a third detection strategy (a Redis Stream consumer
 * group, say, or an external scheduler webhook) can be added without the dispatcher knowing, and
 * the dispatcher can be tested by publishing synthetic firings onto the bus with no Redis in
 * sight.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@Component
public class SlaTimerFiringBus {

    private static final Logger LOG = LoggerFactory.getLogger(SlaTimerFiringBus.class);
    private static final int BUFFER_SIZE = 1_024;

    private final Sinks.Many<SlaTimerFiring> sink =
            Sinks.many().multicast().onBackpressureBuffer(BUFFER_SIZE, false);

    /**
     * Publishes a claimed firing. Failures are logged rather than thrown: a firing has already been
     * atomically claimed in Redis by this point, so propagating an error here would terminate the
     * detection subscription and stop all future alerting over one dropped buffer slot.
     *
     * @param firing the claimed firing; must not be {@code null}
     * @return {@code true} when the firing was accepted onto the bus
     */
    public boolean publish(SlaTimerFiring firing) {
        Objects.requireNonNull(firing, "firing must not be null");
        Sinks.EmitResult result = sink.tryEmitNext(firing);
        if (!result.isSuccess()) {
            LOG.error("SLA firing bus refused table={} sla={} source={} result={}; "
                            + "this breach will not be alerted",
                    firing.tableId(), firing.slaKind(), firing.source(), result);
            return false;
        }
        return true;
    }

    /**
     * @return the hot stream of claimed firings; never completes during normal operation
     */
    public Flux<SlaTimerFiring> firings() {
        return sink.asFlux();
    }
}
