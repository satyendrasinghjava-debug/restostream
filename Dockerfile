# syntax=docker/dockerfile:1.7
#
# © 2026 Satyendra Singh. All Rights Reserved.
# Author: Satyendra Singh
#
# Multi-stage image for RestoStream.
#
# Stage 1 resolves dependencies against the POM alone before any source is copied, so an edit to a
# Java file reuses the cached dependency layer instead of re-downloading the world. Stage 2 then
# unpacks the Spring Boot jar into its four natural layers — dependencies, loader, snapshot
# dependencies, application — and copies them in order of volatility. The effect is that shipping a
# code change pushes a few hundred kilobytes rather than the whole ~60 MB fat jar, because every
# layer below the application layer is bit-identical to the previous build.

# ---------------------------------------------------------------------------
# Stage 1 — build
# ---------------------------------------------------------------------------
FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /build

# Dependency resolution layer: invalidated only by a POM change.
COPY pom.xml ./
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -ntp dependency:go-offline

# Source layer.
COPY src ./src

# Unit tests run inside the image build; the Testcontainers integration suite does not, because it
# needs a Docker daemon of its own. Run `mvn verify` on a machine with Docker for the full suite.
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -ntp clean package

# Unpack into layers using the Spring Boot `tools` jarmode.
RUN java -Djarmode=tools -jar target/restostream.jar extract --layers --launcher --destination extracted

# ---------------------------------------------------------------------------
# Stage 2 — runtime
# ---------------------------------------------------------------------------
FROM eclipse-temurin:21-jre-jammy AS runtime

LABEL org.opencontainers.image.title="RestoStream" \
      org.opencontainers.image.description="Real-time reactive restaurant table management middleware" \
      org.opencontainers.image.version="1.0.0" \
      org.opencontainers.image.authors="Satyendra Singh" \
      org.opencontainers.image.vendor="Satyendra Singh" \
      org.opencontainers.image.licenses="© 2026 Satyendra Singh. All Rights Reserved."

# curl is used by the container healthcheck below; nothing else is added to the runtime image.
RUN apt-get update \
 && apt-get install --no-install-recommends -y curl \
 && rm -rf /var/lib/apt/lists/*

# Never run as root. A numeric UID is used so the image also satisfies a Kubernetes
# `runAsNonRoot` policy, which cannot resolve a username.
RUN groupadd --system --gid 10001 restostream \
 && useradd --system --uid 10001 --gid restostream --home /app --shell /usr/sbin/nologin restostream

WORKDIR /app

# Ordered least-volatile first so that a code change rebuilds only the final layer.
COPY --from=build --chown=10001:10001 /build/extracted/dependencies/ ./
COPY --from=build --chown=10001:10001 /build/extracted/spring-boot-loader/ ./
COPY --from=build --chown=10001:10001 /build/extracted/snapshot-dependencies/ ./
COPY --from=build --chown=10001:10001 /build/extracted/application/ ./

USER 10001:10001

EXPOSE 8080

ENV SPRING_PROFILES_ACTIVE=docker \
    REDIS_HOST=redis \
    REDIS_PORT=6379 \
    SERVER_PORT=8080 \
    JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0 \
-XX:InitialRAMPercentage=50.0 \
-XX:+UseG1GC \
-XX:+ExitOnOutOfMemoryError \
-XX:MaxGCPauseMillis=100 \
-Djava.security.egd=file:/dev/./urandom \
-Dfile.encoding=UTF-8 \
-Duser.timezone=UTC \
-Dreactor.netty.ioWorkerCount=4"

# Readiness, not liveness: the probe reports healthy only once the Redis connection is usable, so an
# orchestrator will not route dashboard traffic at an instance that cannot read table state.
HEALTHCHECK --interval=15s --timeout=4s --start-period=40s --retries=4 \
  CMD curl -fsS http://127.0.0.1:${SERVER_PORT}/actuator/health/readiness || exit 1

ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
