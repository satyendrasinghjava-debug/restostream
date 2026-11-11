/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.support;

import com.satyendra.restostream.config.RestoStreamProperties;
import com.satyendra.restostream.service.TableManagementService;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Base class for the integration suite: starts one real Redis server in Docker and points the
 * application at it.</p>
 *
 * <h2>Why a real Redis rather than an embedded fake</h2>
 * <p>Everything load bearing about the timer subsystem is Redis behaviour that a fake would simply
 * assert into existence: Lua script atomicity, {@code ZRANGEBYSCORE} semantics, {@code HSETNX},
 * {@code PX} key expiry, and the keyspace-expiry notifications that drive sub-second breach
 * detection. Testing a compare-and-set against an in-memory map proves nothing about whether the
 * compare-and-set works.</p>
 *
 * <p>The container is started with {@code notify-keyspace-events Ex}, exactly as the shipped
 * {@code docker/redis/redis.conf} does, so the integration suite exercises the same configuration
 * production runs.</p>
 *
 * <h2>One container for the whole suite</h2>
 * <p>This is Testcontainers' <em>singleton container</em> pattern: the container is {@code static}
 * and started from a static initialiser rather than managed by the {@code @Testcontainers} JUnit
 * extension, which would otherwise tie its lifecycle to each test class. Four integration classes
 * therefore share one container start instead of paying four, and the JVM reaps it on exit via
 * Testcontainers' own Ryuk sidecar.</p>
 *
 * <p>Sharing has a cost that must be paid explicitly: tests can see each other's tables, timers and
 * deadline-index entries. {@link #resetRedis()} therefore flushes the key space and re-provisions
 * the floor plan before every test, which is safe to repeat precisely because provisioning is
 * idempotent.</p>
 *
 * <h2>Why each class gets its own key space, and its context closed</h2>
 * <p>Spring's test framework <em>caches</em> application contexts: without intervention, the context
 * of a finished test class stays alive and keeps running its SLA expiry listener and reconciliation
 * sweeper. Four integration classes against one shared Redis therefore become an accidental
 * four-node cluster — and because {@code DistributedTimerService.claim} is an atomic,
 * exactly-once-cluster-wide operation, a deadline armed by one class's instance could legitimately
 * be claimed and alerted by <em>another</em> class's instance, whose dashboards the test is not
 * watching. The alert was never lost; it was delivered to the wrong node's subscribers.</p>
 *
 * <p>That is correct production behaviour being tripped over by a test harness, so the harness is
 * what changes. Two measures, belt and braces:</p>
 * <ul>
 *   <li>a <strong>distinct {@code restostream.redis.key-prefix} per context</strong>, so one class's
 *       timers are invisible to another's — the same multi-tenancy the prefix exists for; and</li>
 *   <li><strong>{@link DirtiesContext} at class level</strong>, so each instance is shut down when
 *       its class finishes rather than left running for the rest of the suite.</li>
 * </ul>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public abstract class RedisIntegrationTestBase {

    /** Hands every application context its own Redis namespace. */
    private static final AtomicInteger KEY_SPACE = new AtomicInteger();

    /** Pinned image tag: an integration suite that silently follows a moving tag is not a test. */
    private static final DockerImageName REDIS_IMAGE = DockerImageName.parse("redis:7.4-alpine");

    @SuppressWarnings("resource")
    protected static final GenericContainer<?> REDIS = new GenericContainer<>(REDIS_IMAGE)
            .withExposedPorts(6379)
            .withCommand("redis-server",
                    // The flags production runs with, so the suite exercises the real configuration.
                    "--notify-keyspace-events", "Ex",
                    "--maxmemory-policy", "noeviction",
                    // A throwaway container on an isolated Docker network with no bind directive
                    // would otherwise refuse the test's non-loopback connection under Redis
                    // protected mode. The shipped docker/redis/redis.conf sets the same thing.
                    "--protected-mode", "no",
                    // No persistence: a container that lives for one test class has nothing to
                    // recover, and skipping it keeps the suite fast.
                    "--save", "",
                    "--appendonly", "no")
            .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1))
            .withStartupTimeout(Duration.ofSeconds(60));

    static {
        REDIS.start();
    }

    @Autowired
    private ReactiveStringRedisTemplate redisTemplate;

    @Autowired
    private RestoStreamProperties properties;

    @Autowired
    private TableManagementService tableManagementService;

    /**
     * Points the application's Redis client at the container.
     *
     * @param registry the dynamic property registry
     */
    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        // Resolved once per context creation and then held constant, so every property lookup
        // within one context sees the same namespace.
        String keyPrefix = "restostream-it-" + KEY_SPACE.incrementAndGet();
        registry.add("restostream.redis.key-prefix", () -> keyPrefix);
    }

    /**
     * Clears the shared key space so that tests cannot observe each other's tables, timers or
     * deadline index entries.
     */
    @BeforeEach
    void resetRedis() {
        redisTemplate.execute(connection -> connection.serverCommands().flushAll())
                .then()
                .block(Duration.ofSeconds(10));
        // Flushing removes the floor plan the initializer provisioned at startup, so it is
        // re-provisioned here. Provisioning is idempotent, which is exactly why this is safe to
        // repeat before every test.
        Flux.fromIterable(properties.layout().tables())
                .concatMap(tableManagementService::provision)
                .then()
                .block(Duration.ofSeconds(10));
    }

    /**
     * @return the bound application configuration, for destination names and SLA windows
     */
    protected RestoStreamProperties properties() {
        return properties;
    }

    /**
     * @return the reactive template bound to the container, for assertions about stored state
     */
    protected ReactiveStringRedisTemplate redis() {
        return redisTemplate;
    }
}
