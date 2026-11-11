/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.web;

import com.satyendra.restostream.ingestion.CctvMotionSignalSource;
import com.satyendra.restostream.ingestion.MotionIngestOutcome;
import com.satyendra.restostream.ingestion.PushOccupancySignalSource;
import com.satyendra.restostream.web.dto.IngestSignalRequest;
import com.satyendra.restostream.web.dto.MotionAlertRequest;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.util.List;
import java.util.Objects;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>The asynchronous ingestion surface — the HTTP face of the pluggable transport layer, and the
 * endpoint a CCTV video pipeline will post to unchanged.</p>
 *
 * <h2>Accept-and-acknowledge, not accept-and-answer</h2>
 * <p>These endpoints return {@code 202 Accepted} the moment an observation is buffered, without
 * waiting for the state engine, Redis or the broadcast. That is the correct contract for an edge
 * device: a camera emitting several alerts per second per table has nothing useful to do with a
 * table snapshot, and making it wait for one would couple frame rate to Redis latency. Operators who
 * do want the resulting state use {@link TableRestController} instead.</p>
 *
 * <p>The one exception is the motion endpoint, which returns the <em>conditioner's</em> verdict —
 * confirmed, awaiting confirmation, ambiguous, debounced or below the confidence floor. An edge
 * device genuinely benefits from that: it can back off when its alerts are being debounced, and a
 * field engineer can tune a camera's region of interest by watching the dispositions rather than
 * guessing.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@RestController
@RequestMapping("/api/v1/ingest")
public class IngestionController {

    private final PushOccupancySignalSource manualTapSignalSource;
    private final CctvMotionSignalSource cctvMotionSignalSource;
    private final Clock clock;

    /**
     * Creates the controller.
     *
     * @param manualTapSignalSource  the generic push transport
     * @param cctvMotionSignalSource the CCTV motion conditioner
     * @param clock                  the application clock
     */
    public IngestionController(PushOccupancySignalSource manualTapSignalSource,
                               CctvMotionSignalSource cctvMotionSignalSource,
                               Clock clock) {
        this.manualTapSignalSource =
                Objects.requireNonNull(manualTapSignalSource, "manualTapSignalSource must not be null");
        this.cctvMotionSignalSource =
                Objects.requireNonNull(cctvMotionSignalSource, "cctvMotionSignalSource must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    /**
     * Accepts one occupancy signal for asynchronous processing.
     *
     * @param request the observation
     * @return an acknowledgement completing once the signal has been buffered
     */
    @PostMapping("/signals")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<IngestAcknowledgement> ingestSignal(@Valid @RequestBody IngestSignalRequest request) {
        return Mono.fromSupplier(() -> request.toDomain(clock))
                .flatMap(signal -> manualTapSignalSource.publish(signal)
                        .thenReturn(new IngestAcknowledgement(
                                signal.signalId(), signal.tableId(), "accepted")));
    }

    /**
     * Accepts a batch of occupancy signals for asynchronous processing.
     *
     * @param requests the observations
     * @return one acknowledgement per accepted observation
     */
    @PostMapping("/signals/batch")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Flux<IngestAcknowledgement> ingestSignalBatch(
            @Valid @RequestBody List<IngestSignalRequest> requests) {
        return Flux.fromIterable(requests)
                .concatMap(request -> ingestSignal(request));
    }

    /**
     * Accepts one CCTV pixel/motion alert and returns the conditioner's verdict.
     *
     * @param request the alert
     * @return the verdict, including the confirmation streak after processing
     */
    @PostMapping("/cctv/motion")
    public Mono<MotionIngestOutcome> ingestMotionAlert(@Valid @RequestBody MotionAlertRequest request) {
        return Mono.fromSupplier(() -> request.toDomain(clock))
                .flatMap(cctvMotionSignalSource::accept);
    }

    /**
     * Accepts a batch of CCTV pixel/motion alerts — the natural shape for an NVR flushing a frame
     * window across many tables at once.
     *
     * @param requests the alerts, processed in order
     * @return one verdict per alert
     */
    @PostMapping("/cctv/motion/batch")
    public Flux<MotionIngestOutcome> ingestMotionAlertBatch(
            @Valid @RequestBody List<MotionAlertRequest> requests) {
        return Flux.fromIterable(requests)
                .map(request -> request.toDomain(clock))
                .concatMap(cctvMotionSignalSource::accept);
    }

    /**
     * Reports the state of the CCTV conditioner, so a field engineer can confirm a camera is being
     * heard and see how its alerts are being judged.
     *
     * @return the conditioner's counters
     */
    @GetMapping("/cctv/status")
    public Mono<CctvStatus> cctvStatus() {
        return Mono.fromSupplier(() -> new CctvStatus(
                cctvMotionSignalSource.sourceId(),
                cctvMotionSignalSource.enabled(),
                cctvMotionSignalSource.trackedTables(),
                cctvMotionSignalSource.alertsAccepted(),
                cctvMotionSignalSource.signalsEmitted()));
    }

    /**
     * Acknowledgement of a buffered observation.
     *
     * @param signalId the de-duplication key the signal was stored under
     * @param tableId  the table the observation concerned
     * @param status   always {@code accepted}; present so clients can branch on a field rather than
     *                 on the HTTP status alone
     */
    public record IngestAcknowledgement(String signalId, String tableId, String status) {
    }

    /**
     * Counters describing the CCTV motion conditioner.
     *
     * @param sourceId       the transport identifier
     * @param enabled        whether the adapter participates in ingestion
     * @param trackedTables  tables for which confirmation evidence is currently held
     * @param alertsAccepted alerts conditioned since startup
     * @param signalsEmitted occupancy signals emitted since startup
     */
    public record CctvStatus(String sourceId,
                             boolean enabled,
                             int trackedTables,
                             long alertsAccepted,
                             long signalsEmitted) {
    }
}
