/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Entry point for <strong>RestoStream</strong> — a real-time, horizontally scalable reactive
 * middleware for restaurant table management.</p>
 *
 * <h2>What the service does</h2>
 * <p>It maintains the authoritative lifecycle state of every table on a restaurant floor
 * ({@code FREE} green, {@code SEATED} red, {@code UNCLEANED} amber), enforces service-level
 * agreements with cluster-wide distributed timers, and pushes every change to every connected
 * dashboard the instant it happens.</p>
 *
 * <h2>Shape of the system</h2>
 * <pre>
 *   transports            pipeline                                  consumers
 *   ──────────            ────────                                  ─────────
 *   manual tap  ─┐
 *   mobile cam  ─┼─&gt; OccupancySignalSource ─&gt; SignalIngestionGateway
 *   CCTV motion ─┘                                     │
 *   POS / future ─────────────────────────────────────┐ │
 *                                                     ▼ ▼
 *                                          TableManagementService
 *                                            │        │        │
 *                             TableStateEngine   Redis CAS   DistributedTimerService
 *                              (pure function)  (snapshots)   (TTL key + ZSET index)
 *                                                     │                │
 *                                                     ▼                ▼
 *                                          DashboardBroadcaster   SLA firing bus
 *                                              │          │            │
 *                                   reactive STOMP     SSE        SlaAlertDispatcher
 *                                   (dashboards)    (tooling)   (asynchronous alerts)
 * </pre>
 *
 * <h2>No AI and no machine learning, on purpose</h2>
 * <p>Occupancy is judged by thresholding, Schmitt-trigger hysteresis, N-of-N streak confirmation and
 * debouncing — classical signal conditioning that runs in constant time and constant memory per
 * table. There is no model in the hot path, so the real-time guarantee is a property of the
 * arithmetic rather than a hope about inference latency. The ingestion interface is nonetheless
 * abstract, so a CCTV video pipeline can post pixel/motion alerts directly to the same endpoint the
 * simulated trigger uses today, with no change anywhere downstream.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class RestoStreamApplication {

    /**
     * Boots the service.
     *
     * @param args standard Spring Boot command-line arguments
     */
    public static void main(String[] args) {
        SpringApplication.run(RestoStreamApplication.class, args);
    }
}
