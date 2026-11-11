/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.timer;

import com.satyendra.restostream.config.RestoStreamProperties;
import com.satyendra.restostream.domain.SlaKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Redis-backed implementation of {@link DistributedTimerService}.</p>
 *
 * <h2>The three keys, and why all three are necessary</h2>
 * <ul>
 *   <li><strong>{@code :sla:timer:&lt;tableId&gt;}</strong> — a short-lived key carrying the SLA
 *       name with a {@code PX} expiry equal to the SLA window. Its only job is to <em>expire</em>,
 *       because expiry is what Redis will publish a keyspace notification for. This is the
 *       low-latency path: a breach is observed within milliseconds of becoming real.</li>
 *   <li><strong>{@code :sla:deadlines}</strong> — a sorted set scored by deadline epoch
 *       millisecond. This is durable state rather than a message, so it is what makes the subsystem
 *       lossless. Keyspace notifications are fire-and-forget: a disconnected subscriber, a
 *       failover, or a {@code notify-keyspace-events} setting someone changed in production will
 *       all drop them silently. A range query over this set cannot be dropped.</li>
 *   <li><strong>{@code :sla:meta}</strong> — a hash mapping table to SLA class. This exists for a
 *       subtle but decisive reason: <em>when a Redis key expires, its value is already gone.</em>
 *       The expiry notification carries only the key name, so without a sibling record of what kind
 *       of SLA that timer represented, the alert could not be classified.</li>
 * </ul>
 *
 * <h2>Atomicity</h2>
 * <p>Arming touches three keys and claiming performs a read-decide-remove. Both are therefore Lua
 * scripts: Redis executes a script to completion on its event loop with no interleaving, which is
 * exactly the isolation the claim needs in order to be a cluster-wide mutex without a lock.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@Component
public class RedisDistributedTimerService implements DistributedTimerService {

    private static final Logger LOG = LoggerFactory.getLogger(RedisDistributedTimerService.class);

    /**
     * Sentinel returned by the claim script when the caller must not alert. A sentinel is used in
     * preference to a Lua {@code nil} so that the reply type is unconditionally a bulk string and
     * the reactive mapping stays deterministic.
     */
    static final String NOT_CLAIMED = "-";

    /**
     * KEYS[1] timer key, KEYS[2] deadline index, KEYS[3] meta hash.
     * ARGV[1] table id, ARGV[2] SLA kind, ARGV[3] deadline epoch milli, ARGV[4] ttl millis.
     */
    private static final String ARM_SCRIPT = """
            redis.call('SET', KEYS[1], ARGV[2], 'PX', tonumber(ARGV[4]))
            redis.call('ZADD', KEYS[2], tonumber(ARGV[3]), ARGV[1])
            redis.call('HSET', KEYS[3], ARGV[1], ARGV[2])
            return 1
            """;

    /**
     * KEYS[1] timer key, KEYS[2] deadline index, KEYS[3] meta hash. ARGV[1] table id.
     * Returns 1 when a deadline was actually armed and has now been removed.
     */
    private static final String CANCEL_SCRIPT = """
            local removed = redis.call('ZREM', KEYS[2], ARGV[1])
            redis.call('DEL', KEYS[1])
            redis.call('HDEL', KEYS[3], ARGV[1])
            return removed
            """;

    /**
     * KEYS[1] deadline index, KEYS[2] meta hash, KEYS[3] timer key.
     * ARGV[1] table id, ARGV[2] now epoch milli, ARGV[3] the not-claimed sentinel.
     * Returns the claimed SLA kind, or the sentinel when this caller must not alert.
     */
    private static final String CLAIM_SCRIPT = """
            local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
            if not score then
              return ARGV[3]
            end
            if tonumber(score) > tonumber(ARGV[2]) then
              return ARGV[3]
            end
            local kind = redis.call('HGET', KEYS[2], ARGV[1])
            redis.call('ZREM', KEYS[1], ARGV[1])
            redis.call('HDEL', KEYS[2], ARGV[1])
            redis.call('DEL', KEYS[3])
            if not kind then
              return ARGV[3]
            end
            return kind
            """;

    private final ReactiveStringRedisTemplate redis;
    private final RestoStreamProperties.Redis keys;
    private final RedisScript<Long> armScript;
    private final RedisScript<Long> cancelScript;
    private final RedisScript<String> claimScript;

    /**
     * Creates the timer service.
     *
     * @param redis      reactive string template bound to the configured connection factory
     * @param properties the application configuration supplying the key-space layout
     */
    public RedisDistributedTimerService(ReactiveStringRedisTemplate redis,
                                        RestoStreamProperties properties) {
        this.redis = Objects.requireNonNull(redis, "redis must not be null");
        this.keys = Objects.requireNonNull(properties, "properties must not be null").redis();
        this.armScript = RedisScript.of(ARM_SCRIPT, Long.class);
        this.cancelScript = RedisScript.of(CANCEL_SCRIPT, Long.class);
        this.claimScript = RedisScript.of(CLAIM_SCRIPT, String.class);
    }

    @Override
    public Mono<Void> arm(String tableId, SlaKind slaKind, Duration ttl, Instant deadline) {
        Objects.requireNonNull(tableId, "tableId must not be null");
        Objects.requireNonNull(slaKind, "slaKind must not be null");
        Objects.requireNonNull(ttl, "ttl must not be null");
        Objects.requireNonNull(deadline, "deadline must not be null");
        if (ttl.isZero() || ttl.isNegative()) {
            return Mono.error(new IllegalArgumentException(
                    "ttl must be strictly positive but was " + ttl));
        }
        return redis.execute(
                        armScript,
                        List.of(keys.slaTimerKey(tableId), keys.slaDeadlineIndexKey(), keys.slaMetaKey()),
                        List.of(tableId, slaKind.name(),
                                Long.toString(deadline.toEpochMilli()),
                                Long.toString(ttl.toMillis())))
                .then()
                .doOnSuccess(ignored -> LOG.debug("armed {} timer for table={} ttl={} deadline={}",
                        slaKind, tableId, ttl, deadline));
    }

    @Override
    public Mono<Boolean> cancel(String tableId) {
        Objects.requireNonNull(tableId, "tableId must not be null");
        return redis.execute(
                        cancelScript,
                        List.of(keys.slaTimerKey(tableId), keys.slaDeadlineIndexKey(), keys.slaMetaKey()),
                        List.of(tableId))
                .next()
                .defaultIfEmpty(0L)
                .map(removed -> removed > 0L)
                .doOnNext(removed -> {
                    if (removed) {
                        LOG.debug("cancelled armed SLA timer for table={}", tableId);
                    }
                });
    }

    @Override
    public Mono<SlaKind> claim(String tableId, Instant now) {
        Objects.requireNonNull(tableId, "tableId must not be null");
        Objects.requireNonNull(now, "now must not be null");
        return redis.execute(
                        claimScript,
                        List.of(keys.slaDeadlineIndexKey(), keys.slaMetaKey(), keys.slaTimerKey(tableId)),
                        List.of(tableId, Long.toString(now.toEpochMilli()), NOT_CLAIMED))
                .next()
                .filter(result -> !NOT_CLAIMED.equals(result))
                .flatMap(result -> parseKind(tableId, result));
    }

    @Override
    public Flux<String> findOverdue(Instant now, int limit) {
        Objects.requireNonNull(now, "now must not be null");
        if (limit <= 0) {
            return Flux.error(new IllegalArgumentException("limit must be positive but was " + limit));
        }
        Range<Double> dueRange = Range.closed(0.0d, (double) now.toEpochMilli());
        return redis.opsForZSet()
                .rangeByScore(keys.slaDeadlineIndexKey(), dueRange, Limit.limit().count(limit));
    }

    @Override
    public Mono<Instant> deadlineOf(String tableId) {
        Objects.requireNonNull(tableId, "tableId must not be null");
        return redis.opsForZSet()
                .score(keys.slaDeadlineIndexKey(), tableId)
                .map(score -> Instant.ofEpochMilli(score.longValue()));
    }

    @Override
    public Mono<Long> armedCount() {
        return redis.opsForZSet().size(keys.slaDeadlineIndexKey());
    }

    private Mono<SlaKind> parseKind(String tableId, String raw) {
        try {
            return Mono.just(SlaKind.valueOf(raw));
        } catch (IllegalArgumentException ex) {
            LOG.error("claimed an SLA timer for table={} with unrecognised kind '{}'; dropping",
                    tableId, raw);
            return Mono.empty();
        }
    }
}
