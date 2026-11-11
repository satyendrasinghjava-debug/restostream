/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.config;

import com.satyendra.restostream.domain.SignalSourceKind;
import com.satyendra.restostream.ingestion.CctvMotionSignalSource;
import com.satyendra.restostream.ingestion.MotionConditioningPolicy;
import com.satyendra.restostream.ingestion.OccupancySignalSource;
import com.satyendra.restostream.ingestion.PushOccupancySignalSource;
import com.satyendra.restostream.ingestion.SignalIngestionGateway;
import com.satyendra.restostream.service.TableManagementService;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.util.List;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Wires the ingestion transports and the gateway that drives them.</p>
 *
 * <p>The gateway takes {@code List<OccupancySignalSource>}, so adding a transport is a matter of
 * publishing one more bean of that type — here, in another configuration class, or in a separate
 * module on the classpath. Nothing in this file, the state engine or the broadcaster needs to
 * change. That is the practical payoff of the ingestion abstraction: the CCTV adapter below was
 * added the same way a future BLE-beacon or table-weight-sensor adapter will be.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@Configuration(proxyBeanMethods = false)
public class IngestionConfig {

    /**
     * The transport behind the manual dashboard tap, the simulated mobile-camera trigger and the
     * fire-and-forget REST ingestion endpoint.
     *
     * @param properties application configuration
     * @return the push source
     */
    @Bean
    public PushOccupancySignalSource manualTapSignalSource(RestoStreamProperties properties) {
        return new PushOccupancySignalSource(
                "manual-tap", SignalSourceKind.MANUAL_TAP, properties.ingestion().bufferSize());
    }

    /**
     * The CCTV motion adapter. Present whether or not it is enabled, so that its configuration is
     * validated at startup and the REST endpoint can report a clear "disabled" state rather than a
     * missing-bean failure.
     *
     * @param properties application configuration
     * @param clock      the application clock
     * @return the CCTV motion source
     */
    @Bean
    public CctvMotionSignalSource cctvMotionSignalSource(RestoStreamProperties properties,
                                                        Clock clock) {
        RestoStreamProperties.Cctv cctv = properties.ingestion().cctv();
        MotionConditioningPolicy policy = new MotionConditioningPolicy(
                cctv.occupiedMotionRatio(),
                cctv.vacantMotionRatio(),
                cctv.confirmationFrames(),
                cctv.minConfidence(),
                cctv.emitDebounce(),
                cctv.trackRetention());
        return new CctvMotionSignalSource(
                cctv.sourceId(), policy, clock, properties.ingestion().bufferSize(), cctv.enabled());
    }

    /**
     * The gateway that subscribes to every registered transport and drives signals through the
     * orchestration service.
     *
     * @param sources                every signal source bean in the context
     * @param tableManagementService the orchestration service
     * @param meterRegistry          registry for ingestion counters
     * @param clock                  the application clock
     * @param properties             application configuration
     * @return the ingestion gateway
     */
    @Bean
    public SignalIngestionGateway signalIngestionGateway(List<OccupancySignalSource> sources,
                                                         TableManagementService tableManagementService,
                                                         MeterRegistry meterRegistry,
                                                         Clock clock,
                                                         RestoStreamProperties properties) {
        return new SignalIngestionGateway(
                sources,
                tableManagementService,
                meterRegistry,
                clock,
                properties.ingestion().concurrencyPerSource(),
                properties.ingestion().housekeepingInterval());
    }
}
