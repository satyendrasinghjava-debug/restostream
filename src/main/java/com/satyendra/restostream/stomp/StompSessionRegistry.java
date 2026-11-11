/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.stomp;

import com.satyendra.restostream.broadcast.BroadcastKind;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>The broker's routing table: every connected dashboard, and which destinations each one is
 * listening to. This is the component that turns "one state change" into "every screen in the
 * restaurant updates".</p>
 *
 * <p>Delivery is deliberately <strong>non-blocking and never fails a publisher</strong>. Queueing a
 * frame to a session is a bounded offer; a dashboard that cannot keep up is disconnected and the
 * broadcast continues to everyone else. A table transition must never be slowed down, let alone
 * rolled back, because one tablet in the kitchen is on bad Wi-Fi.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@Component
public class StompSessionRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(StompSessionRegistry.class);

    private final ConcurrentHashMap<String, StompClientSession> sessions = new ConcurrentHashMap<>();

    /**
     * Creates the registry and publishes the connected-dashboard gauge.
     *
     * @param meterRegistry registry the gauge is bound to
     */
    public StompSessionRegistry(MeterRegistry meterRegistry) {
        Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
        Gauge.builder("restostream.stomp.sessions", sessions, ConcurrentHashMap::size)
                .description("Dashboards currently connected over STOMP")
                .register(meterRegistry);
    }

    /**
     * Registers a newly accepted session.
     *
     * @param session the session; must not be {@code null}
     */
    public void register(StompClientSession session) {
        Objects.requireNonNull(session, "session must not be null");
        sessions.put(session.sessionId(), session);
        LOG.info("STOMP session {} connected; {} dashboard(s) now attached",
                session.sessionId(), sessions.size());
    }

    /**
     * Removes a session.
     *
     * @param sessionId the session identifier; must not be {@code null}
     */
    public void unregister(String sessionId) {
        Objects.requireNonNull(sessionId, "sessionId must not be null");
        StompClientSession removed = sessions.remove(sessionId);
        if (removed != null) {
            removed.closeQuietly();
            LOG.info("STOMP session {} disconnected; {} dashboard(s) remain attached",
                    sessionId, sessions.size());
        }
    }

    /**
     * @param sessionId the session identifier
     * @return the session, or {@code null} when unknown
     */
    public StompClientSession find(String sessionId) {
        return sessions.get(sessionId);
    }

    /**
     * @return a live view of the connected sessions
     */
    public Collection<StompClientSession> sessions() {
        return sessions.values();
    }

    /**
     * @return the number of connected dashboards
     */
    public int sessionCount() {
        return sessions.size();
    }

    /**
     * Delivers a payload to every subscription, on every connected session, that matches the
     * destination.
     *
     * @param destination the destination being published to; must not be {@code null}
     * @param kind        the payload classification, sent as a frame header
     * @param body        the UTF-8 JSON body; must not be {@code null}
     * @return the number of {@code MESSAGE} frames queued
     */
    public int broadcast(String destination, BroadcastKind kind, byte[] body) {
        Objects.requireNonNull(destination, "destination must not be null");
        return broadcastAll(List.of(destination), kind, body);
    }

    /**
     * Delivers one payload that is addressed to several destinations at once, guaranteeing
     * <strong>at most one frame per subscription</strong>.
     *
     * <h2>Why de-duplication is necessary rather than defensive</h2>
     * <p>A table state change is addressed to two destinations: the aggregate feed
     * {@code /topic/tables} and the table's own feed {@code /topic/tables/T-01}. Ant's {@code **}
     * matches zero or more path segments, so a dashboard that subscribes to
     * {@code /topic/tables/**} — the obvious thing to type — matches <em>both</em>. Publishing each
     * destination independently would therefore deliver every single state change to that dashboard
     * twice, and a tile would flicker or a toast would appear in duplicate for the whole service.</p>
     *
     * <p>Each subscription is delivered on the first destination in the list that it matches.
     * Destinations are supplied aggregate-first, so an exact per-table subscription receives its own
     * destination while a wildcard subscription receives the canonical aggregate one.</p>
     *
     * @param destinations the destinations this payload is addressed to, most general first
     * @param kind         the payload classification, sent as a frame header
     * @param body         the UTF-8 JSON body; must not be {@code null}
     * @return the number of {@code MESSAGE} frames queued
     */
    public int broadcastAll(List<String> destinations, BroadcastKind kind, byte[] body) {
        Objects.requireNonNull(destinations, "destinations must not be null");
        Objects.requireNonNull(body, "body must not be null");
        int delivered = 0;
        for (StompClientSession session : sessions.values()) {
            delivered += deliver(session, destinations, kind, body);
        }
        return delivered;
    }

    /**
     * Delivers a payload to one specific session only — used for command results, which belong to
     * the dashboard that issued the command rather than to the whole restaurant.
     *
     * @param sessionId   the target session; must not be {@code null}
     * @param destination the destination the target is expected to be subscribed to
     * @param kind        the payload classification
     * @param body        the UTF-8 JSON body
     * @return the number of {@code MESSAGE} frames queued; zero when the session is gone or is not
     *         subscribed to the destination
     */
    public int sendTo(String sessionId, String destination, BroadcastKind kind, byte[] body) {
        Objects.requireNonNull(sessionId, "sessionId must not be null");
        StompClientSession session = sessions.get(sessionId);
        if (session == null) {
            return 0;
        }
        return deliver(session, List.of(destination), kind, body);
    }

    /**
     * Queues a heart-beat on every session that asked to receive them.
     *
     * @return the number of heart-beats queued
     */
    public int heartbeatAll() {
        int sent = 0;
        for (StompClientSession session : sessions.values()) {
            if (session.isConnected()
                    && !session.isClosing()
                    && !session.clientHeartbeatReceive().isZero()
                    && session.sendHeartbeat()) {
                sent++;
            }
        }
        return sent;
    }

    private int deliver(StompClientSession session,
                        List<String> destinations,
                        BroadcastKind kind,
                        byte[] body) {
        if (!session.isConnected() || session.isClosing()) {
            return 0;
        }
        Set<String> alreadyDelivered = new HashSet<>(2);
        int delivered = 0;
        for (String destination : destinations) {
            for (String subscriptionId : session.subscriptionsMatching(destination)) {
                if (!alreadyDelivered.add(subscriptionId)) {
                    continue;
                }
                byte[] frame = StompFrameWriter.message(
                        destination, subscriptionId, session.nextMessageId(),
                        kind == null ? null : kind.name(), body);
                if (session.send(frame)) {
                    delivered++;
                }
            }
        }
        return delivered;
    }
}
