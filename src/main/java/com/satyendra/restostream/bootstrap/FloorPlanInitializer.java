/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.bootstrap;

import com.satyendra.restostream.config.RestoStreamProperties;
import com.satyendra.restostream.service.TableManagementService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Provisions the configured floor plan into Redis at startup.</p>
 *
 * <h2>Idempotent by construction</h2>
 * <p>Provisioning uses {@code HSETNX}, so a table that already exists is left exactly as it is. This
 * matters more than it first appears: a rolling deployment at 20:00 on a Saturday restarts every
 * instance in turn, and an initializer that reset tables to {@code FREE} would wipe the live floor
 * plan — telling the host that forty occupied tables are available. Here a restart is a no-op.</p>
 *
 * <h2>Why a SmartLifecycle and not an ApplicationRunner</h2>
 * <p>An {@code ApplicationRunner} executes <em>after</em> the context has fully refreshed, which is
 * after signal ingestion has already started consuming. Tables would briefly not exist while alerts
 * were already arriving. A {@link SmartLifecycle} in an earlier phase runs before the ingestion
 * gateway, so the floor plan is always in place before the first signal is processed.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@Component
public class FloorPlanInitializer implements SmartLifecycle {

    private static final Logger LOG = LoggerFactory.getLogger(FloorPlanInitializer.class);

    /** Earlier than every pipeline component, so the floor plan exists before signals flow. */
    private static final int LIFECYCLE_PHASE = Integer.MAX_VALUE - 10_000;

    /** Startup must not hang indefinitely on an unreachable Redis. */
    private static final Duration PROVISION_TIMEOUT = Duration.ofSeconds(30);

    private final TableManagementService tableManagementService;
    private final RestoStreamProperties properties;
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * Creates the initializer.
     *
     * @param tableManagementService the orchestration service used to provision
     * @param properties             application configuration supplying the floor plan
     */
    public FloorPlanInitializer(TableManagementService tableManagementService,
                                RestoStreamProperties properties) {
        this.tableManagementService =
                Objects.requireNonNull(tableManagementService, "tableManagementService must not be null");
        this.properties = Objects.requireNonNull(properties, "properties must not be null");
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        RestoStreamProperties.Layout layout = properties.layout();
        if (!layout.provisionOnStartup()) {
            LOG.info("floor plan provisioning is disabled by configuration");
            return;
        }
        List<RestoStreamProperties.TableDefinition> tables = layout.tables();
        if (tables.isEmpty()) {
            LOG.warn("no tables are configured under restostream.layout.tables; "
                    + "the floor plan will be empty until tables are provisioned");
            return;
        }

        // Blocking is correct exactly here: this is container startup on the main thread, before any
        // event loop is serving traffic, and the service must not accept signals for tables that do
        // not yet exist. The timeout guarantees a failed Redis surfaces as a fast, clear boot failure.
        Long created = Flux.fromIterable(tables)
                .concatMap(tableManagementService::provision)
                .filter(Boolean::booleanValue)
                .count()
                .timeout(PROVISION_TIMEOUT)
                .block(PROVISION_TIMEOUT);

        LOG.info("floor plan ready: {} table(s) configured, {} newly provisioned",
                tables.size(), created == null ? 0L : created);
    }

    @Override
    public void stop() {
        running.set(false);
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return LIFECYCLE_PHASE;
    }
}
