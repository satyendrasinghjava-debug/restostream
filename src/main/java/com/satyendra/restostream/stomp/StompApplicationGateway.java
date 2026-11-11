/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.stomp;

import reactor.core.publisher.Mono;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>The boundary between the STOMP <em>protocol</em> and the RestoStream <em>application</em>.</p>
 *
 * <p>{@link ReactiveStompWebSocketHandler} owns framing, the handshake, subscriptions, heart-beats
 * and receipts — and knows nothing about tables. The implementation of this interface owns tables
 * and knows nothing about frames. Keeping that line sharp is what allows the broker to be tested
 * with a stub application, and the application to be tested without a WebSocket.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public interface StompApplicationGateway {

    /**
     * Invoked after a {@code SUBSCRIBE} frame has been accepted and registered.
     *
     * <p>The canonical use is to immediately push current state to the new subscriber: a dashboard
     * that attaches mid-service must see the floor plan as it is now, not wait for the next table to
     * change before its tiles light up.</p>
     *
     * @param session        the subscribing session; never {@code null}
     * @param subscriptionId the client's subscription identifier; never {@code null}
     * @param destination    the destination subscribed to; never {@code null}
     * @return a {@link Mono} completing when any initial state has been queued
     */
    Mono<Void> onSubscribed(StompClientSession session, String subscriptionId, String destination);

    /**
     * Invoked for a {@code SEND} frame addressed to an application destination.
     *
     * @param session     the sending session; never {@code null}
     * @param destination the application destination; never {@code null}
     * @param body        the raw frame body; never {@code null}, possibly empty
     * @return a {@link Mono} completing when the command has been processed. Application-level
     *         failures should be reported to the client by the implementation and the {@code Mono}
     *         completed normally; only an infrastructure failure should be signalled as an error.
     */
    Mono<Void> onSend(StompClientSession session, String destination, byte[] body);
}
