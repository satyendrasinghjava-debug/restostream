/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.engine;

import com.satyendra.restostream.domain.OccupancySignal;
import com.satyendra.restostream.domain.SlaKind;
import com.satyendra.restostream.domain.TableSnapshot;
import com.satyendra.restostream.domain.TableState;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>The heart of RestoStream: a <strong>pure, deterministic, side-effect-free</strong> decision
 * function over the table lifecycle. Given the current {@link TableSnapshot}, an incoming
 * {@link OccupancySignal} and the current instant, it returns what <em>should</em> happen — the next
 * snapshot and the timer side effect — without performing any of it.</p>
 *
 * <h2>Why purity is the architectural decision that matters here</h2>
 * <ul>
 *   <li><strong>Testability.</strong> Every transition, guard and race can be exercised with plain
 *       values; no Redis, no event loop, no clock skew. This is what makes a hard 90% line and
 *       branch coverage gate on this package realistic rather than aspirational.</li>
 *   <li><strong>Horizontal correctness.</strong> Because the engine returns a next snapshot carrying
 *       {@code version + 1}, the orchestration layer can persist it with a compare-and-set and
 *       simply re-run the engine on the fresh value if it loses the race. Retry logic stays trivial
 *       precisely because the decision is replayable.</li>
 *   <li><strong>Reactive safety.</strong> A pure function cannot block, so it is safe to invoke
 *       directly inside a WebFlux operator chain on a Netty event-loop thread.</li>
 * </ul>
 *
 * <p>The engine is stateless and therefore a singleton; all mutable state lives in Redis.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@Component
public class TableStateEngine {

    private final SlaPolicy slaPolicy;

    /**
     * Creates the engine.
     *
     * @param slaPolicy supplies the configured timeout per SLA class; must not be {@code null}
     */
    public TableStateEngine(SlaPolicy slaPolicy) {
        this.slaPolicy = Objects.requireNonNull(slaPolicy, "slaPolicy must not be null");
    }

    /**
     * Evaluates one occupancy signal against one table.
     *
     * <p>Evaluation order is significant and deliberate:</p>
     * <ol>
     *   <li><em>Identity</em> — the signal must concern the supplied table. A mismatch is a
     *       programming error in the caller, not an operational conflict, so it throws.</li>
     *   <li><em>Acknowledgement</em> — {@code WAITER_ATTENDED} is handled first because it is the
     *       only signal that disarms an SLA without advancing the state machine.</li>
     *   <li><em>Idempotency</em> — a signal asserting the state the table is already in is ignored
     *       before any guard runs. CCTV motion feeds re-assert the same truth continuously; treating
     *       that as a conflict would flood the dashboard with phantom {@code 409}s.</li>
     *   <li><em>Structural invariant</em> — {@link TableState#canTransitionTo}: is this edge part of
     *       the lifecycle graph at all? Seating guests at an uncleaned table fails here.</li>
     *   <li><em>Operational policy</em> —
     *       {@link com.satyendra.restostream.domain.SignalType#isPermittedFrom}: granted the edge
     *       exists, may <em>this</em> signal drive it? {@code SEATED -> UNCLEANED} is a legal edge,
     *       but only {@code GUEST_DEPARTED} may drive it; a misrouted housekeeping alert fails
     *       here rather than evicting paying guests from the floor plan.</li>
     * </ol>
     *
     * <p>Both guards are reachable, which is deliberate — a guard that cannot fail is dead code
     * dressed up as safety, and it would quietly inflate any coverage gate placed on this class.</p>
     *
     * @param current the table's current persisted state; must not be {@code null}
     * @param signal  the signal to evaluate; must not be {@code null}
     * @param now     the decision instant; must not be {@code null}
     * @return a total decision: applied, ignored or rejected; never {@code null}
     * @throws IllegalArgumentException if the signal does not concern {@code current}
     */
    public TransitionDecision decide(TableSnapshot current, OccupancySignal signal, Instant now) {
        Objects.requireNonNull(current, "current must not be null");
        Objects.requireNonNull(signal, "signal must not be null");
        Objects.requireNonNull(now, "now must not be null");
        if (!current.tableId().equals(signal.tableId())) {
            throw new IllegalArgumentException(
                    "signal for table '%s' cannot be evaluated against table '%s'"
                            .formatted(signal.tableId(), current.tableId()));
        }

        Optional<TableState> maybeTarget = signal.type().targetState();
        if (maybeTarget.isEmpty()) {
            // WAITER_ATTENDED is the only such signal: it disarms an SLA without moving the table.
            return decideAcknowledgement(current, signal, now);
        }
        TableState target = maybeTarget.get();

        if (current.state() == target) {
            return new TransitionDecision.Ignored(current, signal,
                    "table %s is already %s; signal %s from %s is a duplicate assertion and was ignored"
                            .formatted(current.tableId(), current.state(),
                                    signal.type(), signal.sourceKind()));
        }

        if (!current.state().canTransitionTo(target)) {
            return new TransitionDecision.Rejected(current, signal,
                    "transition %s -> %s is not part of the table lifecycle (legal successors of %s are %s)"
                            .formatted(current.state(), target,
                                    current.state(), current.state().allowedSuccessors()));
        }

        if (!signal.type().isPermittedFrom(current.state())) {
            return new TransitionDecision.Rejected(current, signal,
                    "signal %s is only permitted from %s but table %s is %s"
                            .formatted(signal.type(), signal.type().permittedOriginStates(),
                                    current.tableId(), current.state()));
        }

        TimerDirective timer = timerDirectiveFor(target, now);
        TableSnapshot next = current.transitionedTo(
                target, signal, timer.slaKind(), timer.deadline(), now);
        return new TransitionDecision.Applied(current, next, signal, timer,
                "table %s transitioned %s -> %s on %s from %s%s"
                        .formatted(current.tableId(), current.state(), target,
                                signal.type(), signal.sourceKind(), describeTimer(timer)));
    }

    /**
     * Evaluates whether a fired SLA timer still represents a genuine, un-alerted breach.
     *
     * <p>Called from two independent paths — the Redis key-expiry listener and the reconciliation
     * sweeper — which is exactly why it must be idempotent. The {@code slaBreached} guard is what
     * turns an at-least-once timer subsystem into an effectively-once alerting subsystem.</p>
     *
     * @param current   the table's current persisted state; must not be {@code null}
     * @param firedKind the SLA class the fired timer belonged to; must not be {@code null}
     * @param now       the instant the timer fired; must not be {@code null}
     * @return {@link BreachDecision.Confirmed} when the breach is real, otherwise
     *         {@link BreachDecision.Stale}; never {@code null}
     */
    public BreachDecision evaluateBreach(TableSnapshot current, SlaKind firedKind, Instant now) {
        Objects.requireNonNull(current, "current must not be null");
        Objects.requireNonNull(firedKind, "firedKind must not be null");
        Objects.requireNonNull(now, "now must not be null");

        if (!current.slaArmed()) {
            return new BreachDecision.Stale(current,
                    "table %s has no armed SLA; the %s timer that fired was superseded"
                            .formatted(current.tableId(), firedKind));
        }
        if (current.slaKind() != firedKind) {
            return new BreachDecision.Stale(current,
                    "table %s is now guarded by %s; the %s timer that fired belongs to a previous state"
                            .formatted(current.tableId(), current.slaKind(), firedKind));
        }
        if (current.slaBreached()) {
            return new BreachDecision.Stale(current,
                    "table %s has already been alerted for its %s breach; suppressing duplicate"
                            .formatted(current.tableId(), firedKind));
        }
        if (now.isBefore(current.slaDeadline())) {
            return new BreachDecision.Stale(current,
                    "table %s is not due until %s; the %s timer fired %s early"
                            .formatted(current.tableId(), current.slaDeadline(), firedKind,
                                    Duration.between(now, current.slaDeadline())));
        }

        TableSnapshot next = current.breached(now);
        return new BreachDecision.Confirmed(current, next, firedKind,
                "table %s breached its %s SLA; due %s, detected %s (overdue by %s)"
                        .formatted(current.tableId(), firedKind, current.slaDeadline(), now,
                                Duration.between(current.slaDeadline(), now)));
    }

    /**
     * Resolves the timer side effect implied by <em>entering</em> the supplied state.
     *
     * <p>A state that is not SLA tracked, or whose SLA has been switched off in configuration,
     * yields {@link TimerDirective#cancel()} rather than {@link TimerDirective#none()}. That
     * distinction is load bearing: the table is almost certainly carrying a timer armed by the state
     * it is leaving, and failing to clear it would page an operator about an SLA that no longer
     * applies.</p>
     *
     * @param target the state being entered; must not be {@code null}
     * @param now    the transition instant; must not be {@code null}
     * @return the timer directive to execute; never {@code null}
     */
    public TimerDirective timerDirectiveFor(TableState target, Instant now) {
        Objects.requireNonNull(target, "target must not be null");
        Objects.requireNonNull(now, "now must not be null");
        if (!target.slaTracked()) {
            return TimerDirective.cancel();
        }
        SlaKind kind = target.slaKind();
        Optional<Duration> timeout = slaPolicy.timeoutFor(kind);
        if (timeout.isEmpty()) {
            return TimerDirective.cancel();
        }
        Duration ttl = timeout.get();
        if (ttl.isZero() || ttl.isNegative()) {
            return TimerDirective.cancel();
        }
        return TimerDirective.arm(kind, ttl, now.plus(ttl));
    }

    private TransitionDecision decideAcknowledgement(TableSnapshot current,
                                                     OccupancySignal signal,
                                                     Instant now) {
        if (!signal.type().isPermittedFrom(current.state())) {
            return new TransitionDecision.Rejected(current, signal,
                    "waiter acknowledgement is only meaningful for a %s table but table %s is %s"
                            .formatted(signal.type().permittedOriginStates(),
                                    current.tableId(), current.state()));
        }
        if (!current.slaArmed() && current.lastWaiterAckAt() != null) {
            return new TransitionDecision.Ignored(current, signal,
                    "table %s was already acknowledged at %s and carries no armed SLA; ignored"
                            .formatted(current.tableId(), current.lastWaiterAckAt()));
        }
        TableSnapshot next = current.acknowledged(signal, now);
        return new TransitionDecision.Applied(current, next, signal, TimerDirective.cancel(),
                "table %s acknowledged by %s from %s; %s SLA timer cancelled"
                        .formatted(current.tableId(), signal.sourceId(), signal.sourceKind(),
                                current.slaKind() == null ? "no" : current.slaKind().name()));
    }

    /**
     * Renders the timer side effect for the audit narrative. Package-private so that the unit suite
     * can exercise every {@link TimerAction} directly, including the {@link TimerAction#NONE} case
     * that {@link #decide} itself never produces.
     *
     * @param timer the directive to describe; must not be {@code null}
     * @return a narrative fragment, empty for {@link TimerAction#NONE}
     */
    static String describeTimer(TimerDirective timer) {
        return switch (timer.action()) {
            case ARM -> "; %s SLA armed for %s, due %s"
                    .formatted(timer.slaKind(), timer.ttl(), timer.deadline());
            case CANCEL -> "; any armed SLA timer cancelled";
            case NONE -> "";
        };
    }
}
