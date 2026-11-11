/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.engine;

import com.satyendra.restostream.domain.SlaKind;
import com.satyendra.restostream.support.Fixtures;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Unit suite for {@link TimerDirective}. The record's canonical constructor enforces that an
 * {@code ARM} directive is always complete and that a {@code CANCEL} or {@code NONE} directive never
 * smuggles a timer specification — the latter matters because a cancel carrying a stale deadline is
 * exactly the kind of inconsistency that produces a phantom alert hours later.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@DisplayName("TimerDirective")
class TimerDirectiveTest {

    @Test
    @DisplayName("an ARM directive carries a complete, strictly-positive specification")
    void armIsComplete() {
        TimerDirective directive = TimerDirective.arm(
                SlaKind.WAITER_RESPONSE, Fixtures.WAITER_SLA,
                Fixtures.T0.plus(Fixtures.WAITER_SLA));

        assertThat(directive.action()).isEqualTo(TimerAction.ARM);
        assertThat(directive.isArm()).isTrue();
        assertThat(directive.isCancel()).isFalse();
        assertThat(directive.slaKind()).isEqualTo(SlaKind.WAITER_RESPONSE);
        assertThat(directive.ttl()).isEqualTo(Fixtures.WAITER_SLA);
        assertThat(directive.deadline()).isEqualTo(Fixtures.T0.plus(Fixtures.WAITER_SLA));
    }

    @Test
    @DisplayName("CANCEL and NONE are shared, specification-free singletons")
    void cancelAndNoneAreInert() {
        assertThat(TimerDirective.cancel()).isSameAs(TimerDirective.cancel());
        assertThat(TimerDirective.none()).isSameAs(TimerDirective.none());

        assertThat(TimerDirective.cancel().isCancel()).isTrue();
        assertThat(TimerDirective.cancel().isArm()).isFalse();
        assertThat(TimerDirective.cancel().slaKind()).isNull();
        assertThat(TimerDirective.cancel().ttl()).isNull();
        assertThat(TimerDirective.cancel().deadline()).isNull();

        assertThat(TimerDirective.none().action()).isEqualTo(TimerAction.NONE);
        assertThat(TimerDirective.none().isCancel()).isFalse();
        assertThat(TimerDirective.none().isArm()).isFalse();
    }

    @Test
    @DisplayName("refuses an incomplete ARM directive")
    void refusesIncompleteArm() {
        assertThatNullPointerException()
                .isThrownBy(() -> TimerDirective.arm(null, Fixtures.WAITER_SLA, Fixtures.T0))
                .withMessageContaining("slaKind");
        assertThatNullPointerException()
                .isThrownBy(() -> TimerDirective.arm(SlaKind.WAITER_RESPONSE, null, Fixtures.T0))
                .withMessageContaining("ttl");
        assertThatNullPointerException()
                .isThrownBy(() -> TimerDirective.arm(
                        SlaKind.WAITER_RESPONSE, Fixtures.WAITER_SLA, null))
                .withMessageContaining("deadline");
        assertThatNullPointerException()
                .isThrownBy(() -> new TimerDirective(null, null, null, null))
                .withMessageContaining("action");
    }

    @Test
    @DisplayName("refuses a non-positive ARM window")
    void refusesNonPositiveWindow() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> TimerDirective.arm(
                        SlaKind.WAITER_RESPONSE, Duration.ZERO, Fixtures.T0))
                .withMessageContaining("strictly positive");
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> TimerDirective.arm(
                        SlaKind.WAITER_RESPONSE, Duration.ofSeconds(-1), Fixtures.T0))
                .withMessageContaining("strictly positive");
    }

    @Test
    @DisplayName("refuses a CANCEL or NONE directive that carries a timer specification")
    void refusesContradictorySpecification() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new TimerDirective(
                        TimerAction.CANCEL, SlaKind.WAITER_RESPONSE, null, null))
                .withMessageContaining("must not carry a timer specification");
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new TimerDirective(
                        TimerAction.NONE, null, Fixtures.WAITER_SLA, null))
                .withMessageContaining("must not carry a timer specification");
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new TimerDirective(TimerAction.CANCEL, null, null, Fixtures.T0))
                .withMessageContaining("must not carry a timer specification");
    }
}
