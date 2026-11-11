/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.satyendra.restostream.config.RestoStreamProperties;
import com.satyendra.restostream.domain.TableSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Reactive Redis implementation of {@link TableSnapshotRepository}.</p>
 *
 * <h2>Storage layout</h2>
 * <ul>
 *   <li>{@code &lt;prefix&gt;:tables} — hash, field {@code tableId}, value the snapshot as JSON.</li>
 *   <li>{@code &lt;prefix&gt;:tables:v} — hash, field {@code tableId}, value the current version as
 *       a decimal string.</li>
 * </ul>
 *
 * <h2>Why the version lives in its own hash</h2>
 * <p>The compare-and-set is a Lua script, and Lua is the only place Redis gives us multi-key
 * atomicity with a read-decide-write in the middle. Keeping the version as a bare integer in a
 * sibling hash means the script compares numbers with {@code tonumber} instead of parsing the
 * snapshot JSON with {@code cjson}. That is faster, but more importantly it decouples the
 * concurrency-control mechanism from the aggregate's serialisation format — the JSON schema can
 * evolve freely without any risk of breaking the CAS.</p>
 *
 * <p>Both scripts below are single round trips and run atomically on the Redis event loop, so the
 * invariant "version hash and snapshot hash always agree" holds even under crash and failover.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@Repository
public class RedisTableSnapshotRepository implements TableSnapshotRepository {

    private static final Logger LOG = LoggerFactory.getLogger(RedisTableSnapshotRepository.class);

    /**
     * KEYS[1] snapshot hash, KEYS[2] version hash.
     * ARGV[1] table id, ARGV[2] expected version, ARGV[3] snapshot JSON.
     * Returns 1 when applied, 0 when the expected version no longer holds.
     */
    private static final String CAS_SCRIPT = """
            local expected = tonumber(ARGV[2])
            local stored = redis.call('HGET', KEYS[2], ARGV[1])
            local actual = 0
            if stored then
              actual = tonumber(stored)
            end
            if actual ~= expected then
              return 0
            end
            redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
            redis.call('HSET', KEYS[2], ARGV[1], expected + 1)
            return 1
            """;

    /**
     * KEYS[1] snapshot hash, KEYS[2] version hash.
     * ARGV[1] table id, ARGV[2] snapshot JSON, ARGV[3] genesis version.
     * Returns 1 when the table was created, 0 when it already existed.
     */
    private static final String PROVISION_SCRIPT = """
            if redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2]) == 1 then
              redis.call('HSET', KEYS[2], ARGV[1], ARGV[3])
              return 1
            end
            return 0
            """;

    private final ReactiveStringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final RestoStreamProperties.Redis keys;
    private final RedisScript<Long> casScript;
    private final RedisScript<Long> provisionScript;

    /**
     * Creates the repository.
     *
     * @param redis        reactive string template bound to the configured connection factory
     * @param objectMapper the application {@link ObjectMapper}, already configured for java.time
     * @param properties   the application configuration supplying the key-space layout
     */
    public RedisTableSnapshotRepository(ReactiveStringRedisTemplate redis,
                                        ObjectMapper objectMapper,
                                        RestoStreamProperties properties) {
        this.redis = Objects.requireNonNull(redis, "redis must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.keys = Objects.requireNonNull(properties, "properties must not be null").redis();
        this.casScript = RedisScript.of(CAS_SCRIPT, Long.class);
        this.provisionScript = RedisScript.of(PROVISION_SCRIPT, Long.class);
    }

    @Override
    public Mono<TableSnapshot> findById(String tableId) {
        Objects.requireNonNull(tableId, "tableId must not be null");
        return redis.<String, String>opsForHash()
                .get(keys.tablesKey(), tableId)
                .flatMap(this::deserialize);
    }

    @Override
    public Flux<TableSnapshot> findAll() {
        return redis.<String, String>opsForHash()
                .values(keys.tablesKey())
                .flatMap(this::deserialize);
    }

    @Override
    public Mono<Boolean> compareAndSave(TableSnapshot next, long expectedVersion) {
        Objects.requireNonNull(next, "next must not be null");
        if (next.version() != expectedVersion + 1L) {
            return Mono.error(new IllegalArgumentException(
                    "snapshot version %d must be exactly one greater than the expected version %d"
                            .formatted(next.version(), expectedVersion)));
        }
        return serialize(next)
                .flatMap(json -> redis.execute(
                                casScript,
                                List.of(keys.tablesKey(), keys.tableVersionsKey()),
                                List.of(next.tableId(), Long.toString(expectedVersion), json))
                        .next()
                        .defaultIfEmpty(0L))
                .map(applied -> applied == 1L)
                .doOnNext(applied -> {
                    if (!applied) {
                        LOG.debug("CAS lost for table={} at expectedVersion={}; caller will re-read",
                                next.tableId(), expectedVersion);
                    }
                });
    }

    @Override
    public Mono<Boolean> provisionIfAbsent(TableSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        return serialize(snapshot)
                .flatMap(json -> redis.execute(
                                provisionScript,
                                List.of(keys.tablesKey(), keys.tableVersionsKey()),
                                List.of(snapshot.tableId(), json, Long.toString(snapshot.version())))
                        .next()
                        .defaultIfEmpty(0L))
                .map(created -> created == 1L);
    }

    @Override
    public Mono<Long> count() {
        return redis.<String, String>opsForHash().size(keys.tablesKey());
    }

    @Override
    public Mono<Void> deleteAll() {
        return redis.delete(keys.tablesKey(), keys.tableVersionsKey()).then();
    }

    private Mono<String> serialize(TableSnapshot snapshot) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsString(snapshot))
                .onErrorMap(JsonProcessingException.class,
                        cause -> new TableSerializationException(
                                "could not serialise table " + snapshot.tableId(), cause));
    }

    private Mono<TableSnapshot> deserialize(String json) {
        return Mono.fromCallable(() -> objectMapper.readValue(json, TableSnapshot.class))
                .onErrorMap(Exception.class,
                        cause -> new TableSerializationException(
                                "could not deserialise a stored table snapshot", cause));
    }

    /**
     * Raised when a table snapshot cannot be converted to or from its stored JSON form. Surfaced as
     * a distinct type so that a schema-evolution bug is never mistaken for a Redis outage.
     */
    public static class TableSerializationException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        /**
         * @param message explanation
         * @param cause   the underlying Jackson failure
         */
        public TableSerializationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
