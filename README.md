# RestoStream

> © 2026 Satyendra Singh. All Rights Reserved.
> Author: **Satyendra Singh**

**Real-time, horizontally scalable reactive middleware for restaurant table management.**

RestoStream maintains the authoritative lifecycle state of every table on a restaurant floor,
enforces service-level agreements with cluster-wide distributed timers, and pushes every change to
every connected dashboard the instant it happens.

| | |
|---|---|
| **Language / runtime** | Java 21, Spring Boot 3.4 |
| **Web stack** | Spring WebFlux on Reactor Netty — non-blocking end to end |
| **Live channel** | STOMP 1.2 over WebSocket, implemented on the reactive stack |
| **State store** | Redis (reactive Lettuce), optimistic concurrency via Lua compare-and-set |
| **Timers** | Redis TTL keys + a sorted-set deadline index, claimed atomically in Lua |
| **Occupancy detection** | Thresholding, hysteresis, streak confirmation, debounce — **no AI, no ML** |

---

## 1. Why there is no machine learning in the hot path

The requirement was real-time occupancy without heavy models, and that constraint turns out to be
an advantage rather than a compromise.

Occupancy is judged by four classical signal-conditioning rules, executed in constant time and
constant memory per table:

| Rule | What it rejects |
|---|---|
| **Schmitt-trigger hysteresis** — two thresholds with a dead band between them | chatter from motion hovering around a single threshold |
| **N-of-N streak confirmation** | a waiter walking past the camera |
| **Confidence floor** (evidence preserved, not reset) | a momentarily blinded or glare-hit camera |
| **Emission debounce** | write and broadcast amplification at frame rate |

There is no model to load, no GPU to contend for, no inference latency to amortise and no model
version to roll out. The real-time guarantee is a property of the arithmetic. The entire detector is
six numbers in `application.yml` and one class, `CctvMotionSignalSource`.

**It is still camera-ready.** The adapter never sees a frame — only a measurement. A CCTV edge
device, an NVR or an OpenCV background-subtractor posts the same `MotionAlert` payload the simulated
trigger uses today, and nothing downstream changes.

---

## 2. Architecture

```
  transports                    pipeline                             consumers
  ──────────                    ────────                             ─────────
  manual tap ───┐
  mobile cam ───┼──▶ OccupancySignalSource ──▶ SignalIngestionGateway
  CCTV motion ──┘        (the extension seam)          │
  POS / future ────────────────────────────────────────┤
                                                       ▼
                                           TableManagementService
                                      ┌────────────────┼────────────────┐
                                      ▼                ▼                ▼
                            TableStateEngine     Redis CAS      DistributedTimerService
                            (pure function)      (snapshots)    (TTL key + ZSET index)
                                                       │                │
                                                       ▼                ▼
                                            DashboardBroadcaster    SLA firing bus
                                             │             │             │
                                   reactive STOMP        SSE      SlaAlertDispatcher
                                    (dashboards)      (tooling)  (asynchronous alerts)
```

### 2.1 The state machine

```
 FREE ──[GUEST_SEATED]──▶ SEATED ──[GUEST_DEPARTED]──▶ UNCLEANED ──[CLEANING_COMPLETED]──▶ FREE
  │                          │                                                              ▲
  └──[CLEANING_REQUIRED]─────┴──────────────────────────────────────────────────────────────┘
                                   (WAITER_ATTENDED disarms the SLA without moving the table)
```

| State | Colour | SLA while resting here |
|---|---|---|
| `FREE` | 🟢 GREEN `#2E7D32` | none |
| `SEATED` | 🔴 RED `#C62828` | `WAITER_RESPONSE` — **5 minutes** (the specification's rule) |
| `UNCLEANED` | 🟡 AMBER `#F9A825` | `TABLE_TURNAROUND` — 3 minutes (configurable, disableable) |

Transitions are guarded twice, and **both guards are reachable** — a guard that cannot fail is dead
code dressed up as safety:

1. **Structural invariant** (`TableState.canTransitionTo`) — is this edge in the graph at all?
   Seating guests at an uncleaned table fails here.
2. **Operational policy** (`SignalType.isPermittedFrom`) — granted the edge exists, may *this* signal
   drive it? `SEATED → UNCLEANED` is legal, but only `GUEST_DEPARTED` may drive it; a misrouted
   housekeeping alert fails here instead of evicting paying guests from the floor plan.

### 2.2 Three decisions worth defending

**The state engine is a pure function.** `TableStateEngine.decide(snapshot, signal, instant)` performs
no I/O and returns a sealed `TransitionDecision` — `Applied`, `Ignored` or `Rejected`. Three
consequences follow: every transition and race is testable with plain values (hence a real 100%
branch gate); a lost compare-and-set is recovered by simply re-reading and re-deciding, because the
decision is replayable; and it is safe to call on a Netty event-loop thread because it cannot block.

`Ignored` is a first-class outcome, not a convenience. A CCTV feed re-asserts the same truth many
times a minute; treating that as a conflict would flood the dashboard with phantom `409`s and
repeatedly push out SLA deadlines.

**Timers live in Redis, and expiry is detected twice.**

| Key | Role |
|---|---|
| `…:sla:timer:<tableId>` | short-lived key whose only job is to **expire**, so Redis publishes a keyspace notification — the low-latency path (measured: **19.7 ms** after the deadline) |
| `…:sla:deadlines` | sorted set scored by deadline; **state, not a message**, so a range query cannot be dropped — the lossless path |
| `…:sla:meta` | table → SLA class. This exists because **an expired key's value is already gone**: the notification carries only the key name |

`claim` is a single Lua script performing read-remove atomically, so when three instances all observe
the same expiry, exactly one is told to alert — no leader election, no distributed lock. Remove
`notify-keyspace-events Ex` and RestoStream still loses **no** alerts; detection latency simply
degrades to one sweep interval. A misconfigured cache must never be able to silence service alerts.

**Persist, then arm, then broadcast.** Letting the compare-and-set adjudicate first means only the
*winner* performs side effects. The one window this leaves — a crash after the write but before the
timer is armed — is closed by `TimerDriftReconciler`, which repairs drift in both directions
(re-arming a snapshot nothing is counting down, cancelling a deadline for a table no longer subject
to it) rather than being documented as a known limitation.

### 2.3 Reactive STOMP, and why it is hand-rolled

Spring's familiar STOMP support (`@EnableWebSocketMessageBroker`, `SimpMessagingTemplate`,
`@MessageMapping`) is **Servlet-stack only and does not run on WebFlux**. A system specified as a
reactive state manager *and* a STOMP broadcaster must therefore choose: abandon WebFlux, or implement
the protocol on the reactive stack. RestoStream does the latter, because a live dashboard broadcaster
is exactly the workload where the reactive model pays — thousands of mostly-idle connections fanned
out to on a handful of event-loop threads, with no thread parked per dashboard.

The implementation is deliberately asymmetric:

* **Inbound** uses Spring's own production `StompDecoder` (transport-agnostic, lives in
  `spring-messaging`), because parsing is the hard, adversarial direction.
* **Outbound** is written explicitly in `StompFrameWriter`, because the grammar is a dozen lines and
  writing it means the exact bytes on the wire are visible in one file and asserted byte-for-byte in
  tests. Header escaping is unconditional — an unescaped colon in a client-supplied `receipt` would
  let a client forge a header into a frame addressed to another subscriber.

`StompFrameAccumulator` exists because **a WebSocket message boundary is not a STOMP frame
boundary**: a conforming client may split one frame across two messages or pack three into one. A
broker that assumes otherwise passes against the common JavaScript client and then corrupts traffic
the first time a payload fragments.

Fan-out de-duplicates per subscription. Ant's `**` matches *zero* or more segments, so a dashboard
subscribing to `/topic/tables/**` matches both destinations a state change is addressed to; without
`broadcastAll`'s de-duplication every change would arrive twice.

---

## 3. Running it

### 3.1 Docker Compose (one command)

```bash
docker compose up --build
open http://localhost:8080          # the live dashboard
```

Horizontal scale-out — two instances sharing one Redis, competing for SLA timer claims:

```bash
docker compose --profile scale-out up --build    # :8080 and :8081
```

### 3.2 Locally

```bash
redis-server --notify-keyspace-events Ex --maxmemory-policy noeviction
mvn spring-boot:run
```

### 3.3 Try it

```bash
# floor plan
curl -s localhost:8080/api/v1/tables | jq '.[] | {tableId, state, colourName}'

# seat a table — arms the 5-minute waiter SLA
curl -s -X POST 'localhost:8080/api/v1/tables/T-01/seat?occupancy=3&actor=waiter-7' | jq

# a waiter attends — cancels the timer
curl -s -X POST 'localhost:8080/api/v1/tables/T-01/waiter-ack?actor=waiter-7' | jq

# watch everything, with no client library
curl -N -H 'Accept: text/event-stream' localhost:8080/api/v1/tables/stream

# a CCTV pixel/motion alert (post it three times to confirm)
curl -s -X POST localhost:8080/api/v1/ingest/cctv/motion \
  -H 'content-type: application/json' \
  -d '{"tableId":"T-02","cameraId":"cam-patio-3","motionRatio":0.42,"confidence":0.9,"occupancyHint":2}' | jq
```

Watch an SLA fire in seconds instead of minutes:

```bash
mvn spring-boot:run -Dspring-boot.run.arguments=--restostream.sla.waiter-response=5s
```

---

## 4. API

### REST — commands and queries (synchronous; the caller is waiting)

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/v1/tables` | the whole floor plan, stably ordered |
| `GET` | `/api/v1/tables/{id}` | one table |
| `POST` | `/api/v1/tables/{id}/signals` | raise any signal (JSON body) |
| `POST` | `/api/v1/tables/{id}/seat?occupancy=&actor=` | guests seated |
| `POST` | `/api/v1/tables/{id}/waiter-ack?actor=` | **the waiter event that cancels the SLA** |
| `POST` | `/api/v1/tables/{id}/depart?actor=` | guests left |
| `POST` | `/api/v1/tables/{id}/cleaned?actor=` | bussing finished |
| `GET` | `/api/v1/tables/stream` | Server-Sent Events mirror of the broadcast |

### REST — ingestion (asynchronous; `202`, nothing is waiting)

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/v1/ingest/signals` · `/batch` | fire-and-forget occupancy signals |
| `POST` | `/api/v1/ingest/cctv/motion` · `/batch` | **pixel/motion alerts — the CCTV contract** |
| `GET` | `/api/v1/ingest/cctv/status` | conditioner counters, for tuning a camera in the field |

The motion endpoint returns the conditioner's verdict (`EMITTED`, `AWAITING_CONFIRMATION`,
`AMBIGUOUS`, `LOW_CONFIDENCE`, `DEBOUNCED`, `DUPLICATE_ASSERTION`) so an edge device can back off and
a field engineer can tune a region of interest by watching dispositions instead of guessing.

### Diagnostics

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/v1/diagnostics` | tables, armed timers, attached dashboards, sweep counters |
| `POST` | `/api/v1/diagnostics/sweep` | force one reconciliation sweep |
| `POST` | `/api/v1/diagnostics/reconcile-timers` | force one drift reconciliation pass |

Both `POST`s are idempotent and safe in production: an operator investigating a suspected missed
alert can prove in one call whether the deadline index agrees with the snapshot store.

### STOMP — `ws://host:8080/ws/stomp` (sub-protocol `v12.stomp`)

| Direction | Destination | Payload |
|---|---|---|
| subscribe | `/topic/tables` | `FLOOR_SNAPSHOT` on subscribe, then every `TABLE_STATE` change |
| subscribe | `/topic/tables/{id}` | that table only (`/topic/tables/**` also works) |
| subscribe | `/topic/sla-alerts` | `SLA_ALERT` |
| subscribe | `/topic/timers` | `TIMER_TICK` — the server-authoritative countdown |
| subscribe | `/topic/command-results` | the result of **this session's own** commands |
| send | `/app/tables/signal` | `{tableId, signal, occupancy?, actor?, correlationId?}` |
| send | `/app/floor/refresh` | re-send the floor plan |

A new subscriber is **primed immediately** with current state. Without that, a dashboard opened at
19:00 would render a blank grid until the next table happened to change.

Business outcomes come back as an ordinary `MESSAGE` on `/topic/command-results`, never as an
`ERROR` frame — `ERROR` is terminal in STOMP, and tearing down a live feed because a waiter
double-tapped a tile would be absurd. `ERROR` is reserved for genuine protocol violations.

Errors use one shape everywhere, so a dashboard has one failure branch:

| Status | `error` | Retryable |
|---|---|---|
| `404` | `table_not_found` | no |
| `409` | `invalid_transition` | **no** — the floor plan contradicts the request |
| `409` + `Retry-After` | `write_contention` | **yes** — lost its CAS race repeatedly |
| `503` + `Retry-After` | `ingestion_saturated` / `state_store_unavailable` | yes |

---

## 5. Tests

```bash
mvn test       # 295 unit tests, no Docker needed, no sleeping
mvn verify     # + 46 Testcontainers integration tests = 341 total (needs a Docker daemon)
```

Both are green: **341 tests, 0 failures**, in about 1m40s on a 4-core machine.

> **Docker API version.** Testcontainers is pinned to 1.21.4 rather than an older 1.20.x because
> Docker Engine 29 removed support for API versions below 1.40, while docker-java in 1.20.x still
> negotiates 1.32 and fails with *"client version 1.32 is too old"*. If you are on an older engine,
> any 1.20.x+ works.

### Unit suite — 295 tests

Nothing in RestoStream calls `Instant.now()`; every timestamp comes from an injected `Clock`. That
single discipline is why a five-minute SLA, a twenty-second debounce and a thirty-minute track
retention are all asserted inside the same millisecond of real time, with **no `Thread.sleep` and no
timing flakiness** anywhere in the unit suite.

| Area | Highlights |
|---|---|
| `TableStateEngine` | the complete **3-state × 5-signal decision matrix** swept parameterically, plus each guard individually; every breach race (acknowledged, superseded SLA, already alerted, fired early) |
| `CctvMotionSignalSource` | each conditioning rule in isolation and in combination, driven by a hand-advanced clock |
| `TableManagementService` | ordering and contention: persist→arm→broadcast order, re-**decide** (not replay) on a lost CAS, attempt-budget exhaustion, timer failure not rolling back a committed transition |
| STOMP broker | driven end-to-end over an in-memory `WebSocketSession`: real frames in, real frames out. Unhappy paths covered as thoroughly as the happy one |
| `StompFrameWriter` | whole frames compared against literal expected bytes, including header-injection defence |

Coverage gates are enforced by JaCoCo in the `test` phase and **fail the build**:

| Package | Gate | Achieved (unit tests only) |
|---|---|---|
| `…engine` | 90% line **and** branch | **100% line / 100% branch** |
| `…domain` | 80% line | **100% line / 96.4% branch** |

The gates deliberately measure **unit-test data alone**. A gate that counts integration tests can be
satisfied by one happy-path walk-through, which is not what a 90% engine gate is meant to assert.
`mvn verify` additionally writes a merged report to `target/site/jacoco-merged` covering both suites:

| Package | Line | Branch |  | Package | Line | Branch |
|---|---|---|---|---|---|---|
| `…engine` | **100.0%** | **100.0%** | | `…stomp` | 88.8% | 79.4% |
| `…domain` | 100.0% | 96.4% | | `…ingestion` | 89.1% | 80.6% |
| `…service` | 97.8% | 86.4% | | `…bootstrap` | 83.9% | 50.0% |
| `…broadcast` | 96.6% | 75.0% | | `…repository` | 80.0% | 50.0% |
| `…config` | 95.0% | 55.6% | | `…web` | 68.9% | 33.3% |
| `…timer` | 92.0% | 77.4% | | `…alert` | 90.1% | 64.3% |
| | | | | **TOTAL** | **90.1%** | **77.7%** |

### Integration suite — Testcontainers

A real Redis container, started with `notify-keyspace-events Ex` exactly as production runs, because
everything load-bearing here is Redis behaviour a fake would merely assert into existence: Lua
atomicity, `ZRANGEBYSCORE` semantics, `HSETNX`, `PX` expiry, keyspace notifications. Testing a
compare-and-set against an in-memory map proves nothing about whether the compare-and-set works.

| Class | Tests | Proves |
|---|---|---|
| `TableLifecycleIntegrationTest` | 11 | lifecycle through the real HTTP stack; Lua CAS keeps the version hash in step; `409`/`404`/validation through the whole WebFlux chain |
| `StompBroadcastIntegrationTest` | 14 | a real WebSocket and a **separately implemented** STOMP client — so a symmetric bug cannot pass unnoticed |
| `SlaExpiryIntegrationTest` | 11 | the headline requirement: seated + no waiter event ⇒ asynchronous alert; **exactly one** alert per breach however many passes observe it; drift repair |
| `CctvIngestionIntegrationTest` | 10 | a camera drives the whole transition, indistinguishable downstream from a manual tap |

**Each class gets its own Redis key prefix and its context closed after the class.** This is not
boilerplate — Spring caches test contexts, so without it four finished classes keep running their
expiry listeners and sweepers against one shared Redis, forming an accidental four-node cluster.
Since `claim` is atomic and exactly-once *cluster-wide*, a deadline armed by one class's instance
would then be legitimately claimed and alerted by **another** class's instance, whose dashboards the
test is not watching — correct production behaviour, tripping up the harness. The fix belongs in the
harness, and `RedisIntegrationTestBase` documents it at the point of use.

---

## 6. Configuration

Every setting binds into an immutable record tree (`RestoStreamProperties`) and is validated at
startup, so a mistyped key fails the boot instead of defaulting to zero at runtime.

```yaml
restostream:
  sla:
    waiter-response: 5m        # the specification's rule; 0 disables
    table-turnaround: 3m
  timers:
    keyspace-notifications-enabled: true   # latency optimisation
    sweep-enabled: true                    # the delivery guarantee
    sweep-interval: 1s
    countdown-interval: 5s
    drift-reconcile-interval: 30s
  ingestion:
    cctv:
      occupied-motion-ratio: 0.18   # at/above => occupied
      vacant-motion-ratio: 0.04     # at/below => vacant   (between => dead band)
      confirmation-frames: 3
      min-confidence: 0.45
      emit-debounce: 20s
  stomp:
    endpoint: /ws/stomp
    heartbeat-send: 10s
```

`REDIS_HOST`, `REDIS_PORT`, `SERVER_PORT`, `RESTOSTREAM_SLA_WAITER_RESPONSE` and
`RESTOSTREAM_SLA_TABLE_TURNAROUND` are honoured as environment variables.

---

## 7. Deployment notes

The image is multi-stage: dependencies resolve against the POM alone before any source is copied, and
the Spring Boot jar is unpacked into its four natural layers and copied in order of volatility — so
shipping a code change pushes a few hundred kilobytes rather than the whole ~38 MB fat jar. The
runtime stage is a JRE on `eclipse-temurin:21-jre-jammy`, runs as **non-root UID 10001** (numeric, so
a Kubernetes `runAsNonRoot` policy can resolve it), and health-checks against
`/actuator/health/readiness` — readiness rather than liveness, so an orchestrator will not route
dashboard traffic at an instance that cannot read table state.

Two Redis settings are not optional:

* `notify-keyspace-events Ex` — without it, breach detection degrades from sub-second to one sweep
  interval. No alerts are lost.
* `maxmemory-policy noeviction` — **mandatory.** Under `allkeys-lru` Redis could evict an SLA timer
  or a table snapshot to reclaim memory, and a silently evicted deadline is an alert that never
  fires. Failing writes loudly is strictly better than losing state quietly.

Observability: `/actuator/health`, `/actuator/prometheus`, and the counters
`restostream.ingestion.signals`, `restostream.broadcast.envelopes`, `restostream.sla.firings` (tagged
`detectedBy`, so the ratio between `KEYSPACE_EXPIRY` and `RECONCILIATION_SWEEP` reveals silently
degraded breach latency) and the `restostream.stomp.sessions` gauge.

---

## 8. Extending it

To add a transport — a POS integration, a BLE beacon, a table weight sensor, a different CCTV
protocol — publish one bean:

```java
@Bean
OccupancySignalSource posTicketSource(/* your client */) {
    return new PosTicketSignalSource(...);   // implements OccupancySignalSource
}
```

`SignalIngestionGateway` injects `List<OccupancySignalSource>`, discovers it by type at startup and
merges it in. Nothing in the state engine, the timers, the broadcaster or the dashboard changes — and
each source gets its **own** subscription, retry policy and metrics, so one misbehaving camera
adapter cannot take the manual-tap transport down with it and stop the restaurant seating guests.

---

## 9. Layout

```
restostream/
├── pom.xml                      Java 21, Spring Boot 3.4, JaCoCo gates, Surefire/Failsafe split
├── Dockerfile                   multi-stage, layered, non-root, healthcheck
├── docker-compose.yml           service + Redis (+ optional scale-out instance)
├── docker/redis/redis.conf      keyspace events, AOF, noeviction
└── src/
    ├── main/java/com/satyendra/restostream/
    │   ├── domain/              TableState, TableSnapshot, OccupancySignal, SignalType, SlaKind
    │   ├── engine/              TableStateEngine (pure), sealed decisions, TimerDirective
    │   ├── ingestion/           OccupancySignalSource seam, CCTV conditioner, gateway
    │   ├── repository/          Lua compare-and-set snapshot store
    │   ├── timer/               Redis timers, expiry listener, sweeper, drift reconciler
    │   ├── alert/               SlaAlert, severity, asynchronous dispatcher
    │   ├── broadcast/           transport-agnostic fan-out, countdown publisher
    │   ├── stomp/               reactive STOMP 1.2 broker + live broadcast controller
    │   ├── service/             TableManagementService (the only writer)
    │   ├── web/                 REST controllers, DTOs, global error handler
    │   ├── config/              typed properties, bean wiring
    │   └── bootstrap/           idempotent floor-plan provisioning
    ├── main/resources/
    │   ├── application.yml
    │   └── static/index.html    dependency-free live dashboard (STOMP client inline)
    └── test/java/…              295 unit tests + Testcontainers integration suite
```

The dashboard at `/` has **no CDN dependency**: its STOMP 1.2 client is implemented inline on the
native WebSocket API, so the container is self-contained and the wire protocol the Java broker speaks
is visible end to end in one file. It renders the floor as coloured tiles, counts SLA timers down,
raises alert toasts, lets you tap tiles to drive transitions over STOMP, and includes a CCTV motion
simulator with a live ratio slider for exercising the conditioner.

---

© 2026 Satyendra Singh. All Rights Reserved.
