/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.web;

import com.satyendra.restostream.engine.InvalidTransitionException;
import com.satyendra.restostream.ingestion.PushOccupancySignalSource;
import com.satyendra.restostream.repository.RedisTableSnapshotRepository;
import com.satyendra.restostream.service.ConcurrencyExhaustedException;
import com.satyendra.restostream.service.TableNotFoundException;
import com.satyendra.restostream.stomp.StompProtocolException;
import com.satyendra.restostream.web.dto.ApiError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebInputException;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Translates every failure the pipeline can raise into one predictable {@link ApiError} body with
 * the right status code.</p>
 *
 * <p>The status mapping is the interesting part, because three different conditions all look like
 * "conflict" and only two of them are worth retrying:</p>
 * <ul>
 *   <li>{@link InvalidTransitionException} → {@code 409} and <strong>never</strong> retry. The floor
 *       plan genuinely contradicts the request; retrying will fail identically forever.</li>
 *   <li>{@link ConcurrencyExhaustedException} → {@code 409} <em>with</em> {@code Retry-After}. The
 *       request was legal but lost its race repeatedly; a backed-off retry will likely succeed.</li>
 *   <li>{@link PushOccupancySignalSource.SignalRejectedException} → {@code 503} with
 *       {@code Retry-After}. The pipeline is saturated or shutting down; this is pure
 *       backpressure.</li>
 * </ul>
 * <p>Collapsing these into one status, as a default error handler would, is what turns a transient
 * contention spike into a dashboard that silently stops updating.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    private final Clock clock;

    /**
     * Creates the handler.
     *
     * @param clock the application clock used to stamp error bodies
     */
    public GlobalExceptionHandler(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    /**
     * @param exception the raised exception
     * @param exchange  the current exchange
     * @return {@code 404} for an unprovisioned table
     */
    @ExceptionHandler(TableNotFoundException.class)
    public Mono<ResponseEntity<ApiError>> onTableNotFound(TableNotFoundException exception,
                                                          ServerWebExchange exchange) {
        return Mono.just(ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(ApiError.of(HttpStatus.NOT_FOUND.value(), "table_not_found",
                        exception.getMessage(), path(exchange), now())));
    }

    /**
     * @param exception the raised exception
     * @param exchange  the current exchange
     * @return {@code 409} for a transition the state machine refuses
     */
    @ExceptionHandler(InvalidTransitionException.class)
    public Mono<ResponseEntity<ApiError>> onInvalidTransition(InvalidTransitionException exception,
                                                               ServerWebExchange exchange) {
        ApiError body = new ApiError(HttpStatus.CONFLICT.value(), "invalid_transition",
                exception.getMessage(), path(exchange),
                List.of("table=" + exception.tableId(),
                        "currentState=" + exception.currentState(),
                        "signal=" + exception.signalType()),
                now());
        return Mono.just(ResponseEntity.status(HttpStatus.CONFLICT).body(body));
    }

    /**
     * @param exception the raised exception
     * @param exchange  the current exchange
     * @return {@code 409} with a {@code Retry-After} hint for sustained write contention
     */
    @ExceptionHandler(ConcurrencyExhaustedException.class)
    public Mono<ResponseEntity<ApiError>> onConcurrencyExhausted(ConcurrencyExhaustedException exception,
                                                                  ServerWebExchange exchange) {
        ApiError body = new ApiError(HttpStatus.CONFLICT.value(), "write_contention",
                exception.getMessage(), path(exchange),
                List.of("table=" + exception.tableId(), "attempts=" + exception.attempts(),
                        "retryable=true"),
                now());
        return Mono.just(ResponseEntity.status(HttpStatus.CONFLICT)
                .header(HttpHeaders.RETRY_AFTER, "1")
                .body(body));
    }

    /**
     * @param exception the raised exception
     * @param exchange  the current exchange
     * @return {@code 503} with a {@code Retry-After} hint when the ingestion pipeline is saturated
     */
    @ExceptionHandler(PushOccupancySignalSource.SignalRejectedException.class)
    public Mono<ResponseEntity<ApiError>> onSignalRejected(
            PushOccupancySignalSource.SignalRejectedException exception,
            ServerWebExchange exchange) {
        LOG.warn("ingestion refused a signal: {}", exception.getMessage());
        return Mono.just(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, "2")
                .body(ApiError.of(HttpStatus.SERVICE_UNAVAILABLE.value(), "ingestion_saturated",
                        exception.getMessage(), path(exchange), now())));
    }

    /**
     * @param exception the raised validation failure
     * @param exchange  the current exchange
     * @return {@code 400} with one detail line per rejected field
     */
    @ExceptionHandler(WebExchangeBindException.class)
    public Mono<ResponseEntity<ApiError>> onValidationFailure(WebExchangeBindException exception,
                                                               ServerWebExchange exchange) {
        List<String> details = exception.getFieldErrors().stream()
                .map(GlobalExceptionHandler::describe)
                .toList();
        ApiError body = new ApiError(HttpStatus.BAD_REQUEST.value(), "validation_failed",
                "the request body failed validation", path(exchange), details, now());
        return Mono.just(ResponseEntity.badRequest().body(body));
    }

    /**
     * @param exception the raised input failure
     * @param exchange  the current exchange
     * @return {@code 400} for a malformed or undecodable request
     */
    @ExceptionHandler(ServerWebInputException.class)
    public Mono<ResponseEntity<ApiError>> onMalformedInput(ServerWebInputException exception,
                                                            ServerWebExchange exchange) {
        String reason = exception.getReason() == null
                ? "the request could not be read" : exception.getReason();
        return Mono.just(ResponseEntity.badRequest()
                .body(ApiError.of(HttpStatus.BAD_REQUEST.value(), "malformed_request",
                        reason, path(exchange), now())));
    }

    /**
     * @param exception the raised exception
     * @param exchange  the current exchange
     * @return {@code 400} for an argument that violated a domain invariant
     */
    @ExceptionHandler({IllegalArgumentException.class, StompProtocolException.class})
    public Mono<ResponseEntity<ApiError>> onIllegalArgument(RuntimeException exception,
                                                             ServerWebExchange exchange) {
        return Mono.just(ResponseEntity.badRequest()
                .body(ApiError.of(HttpStatus.BAD_REQUEST.value(), "invalid_argument",
                        exception.getMessage() == null
                                ? "the request violated a domain invariant" : exception.getMessage(),
                        path(exchange), now())));
    }

    /**
     * @param exception the raised exception
     * @param exchange  the current exchange
     * @return {@code 503} when the state store is unreachable or its contents are unreadable
     */
    @ExceptionHandler({DataAccessException.class,
            RedisTableSnapshotRepository.TableSerializationException.class})
    public Mono<ResponseEntity<ApiError>> onStateStoreFailure(RuntimeException exception,
                                                               ServerWebExchange exchange) {
        LOG.error("state store failure while handling {}", path(exchange), exception);
        return Mono.just(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, "5")
                .body(ApiError.of(HttpStatus.SERVICE_UNAVAILABLE.value(), "state_store_unavailable",
                        "the table state store could not be reached", path(exchange), now())));
    }

    /**
     * Preserves the status of an exception the framework or a handler raised deliberately, so that a
     * considered {@code 404} or {@code 415} is not flattened into {@code 500} by the catch-all below.
     *
     * @param exception the raised exception
     * @param exchange  the current exchange
     * @return a response carrying the exception's own status
     */
    @ExceptionHandler(ResponseStatusException.class)
    public Mono<ResponseEntity<ApiError>> onResponseStatus(ResponseStatusException exception,
                                                            ServerWebExchange exchange) {
        HttpStatus status = HttpStatus.resolve(exception.getStatusCode().value()) == null
                ? HttpStatus.INTERNAL_SERVER_ERROR
                : HttpStatus.valueOf(exception.getStatusCode().value());
        String reason = exception.getReason() == null ? status.getReasonPhrase() : exception.getReason();
        return Mono.just(ResponseEntity.status(status)
                .body(ApiError.of(status.value(),
                        status.name().toLowerCase(java.util.Locale.ROOT),
                        reason, path(exchange), now())));
    }

    /**
     * @param exception the unexpected exception
     * @param exchange  the current exchange
     * @return {@code 500} with a deliberately generic message
     */
    @ExceptionHandler(Throwable.class)
    public Mono<ResponseEntity<ApiError>> onUnexpected(Throwable exception,
                                                        ServerWebExchange exchange) {
        LOG.error("unhandled failure while handling {}", path(exchange), exception);
        return Mono.just(ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiError.of(HttpStatus.INTERNAL_SERVER_ERROR.value(), "internal_error",
                        "an unexpected error occurred", path(exchange), now())));
    }

    private static String describe(FieldError error) {
        return "%s: %s".formatted(error.getField(),
                error.getDefaultMessage() == null ? "is invalid" : error.getDefaultMessage());
    }

    private static String path(ServerWebExchange exchange) {
        return exchange.getRequest().getPath().value();
    }

    private Instant now() {
        return clock.instant();
    }
}
