/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.broadcast;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>One thing to broadcast, addressed to one or more STOMP destinations.</p>
 *
 * <p>The envelope deliberately carries the payload as an <em>object</em> rather than as serialised
 * bytes. Serialisation is deferred to each transport: the STOMP layer renders it as a JSON frame
 * body, the Server-Sent Events endpoint renders it as an SSE data line, and a future transport may
 * render it as something else entirely. Serialising eagerly here would force every consumer to
 * agree on one encoding and would pay the cost even when nobody is listening.</p>
 *
 * <p>Multiple destinations are carried in one envelope because a single state change is genuinely
 * addressed to several audiences at once — the aggregate feed every dashboard watches, and the
 * per-table feed a single table's tile watches.</p>
 *
 * @param destinations the STOMP destinations this payload is addressed to; never empty
 * @param kind         the payload classification
 * @param payload      the payload object, serialised per transport
 * @param emittedAt    the instant this envelope was produced
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public record BroadcastEnvelope(List<String> destinations,
                                BroadcastKind kind,
                                Object payload,
                                Instant emittedAt) {

    /** Canonical constructor enforcing a non-empty destination list and a non-null payload. */
    public BroadcastEnvelope {
        Objects.requireNonNull(destinations, "destinations must not be null");
        Objects.requireNonNull(kind, "kind must not be null");
        Objects.requireNonNull(payload, "payload must not be null");
        Objects.requireNonNull(emittedAt, "emittedAt must not be null");
        if (destinations.isEmpty()) {
            throw new IllegalArgumentException("destinations must not be empty");
        }
        destinations = List.copyOf(destinations);
    }
}
