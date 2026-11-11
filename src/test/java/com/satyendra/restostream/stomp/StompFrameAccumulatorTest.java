/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.stomp;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Unit suite for frame reassembly.</p>
 *
 * <p>The tests here encode the single most commonly mishandled fact about STOMP over WebSocket: a
 * WebSocket message boundary is not a frame boundary. A broker that assumes otherwise passes against
 * a well-behaved client and then corrupts traffic the first time a client coalesces its writes or a
 * large payload is fragmented, so both directions are asserted explicitly — split frames and
 * coalesced frames, including a split that falls inside the body.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@DisplayName("StompFrameAccumulator")
class StompFrameAccumulatorTest {

    private static final char NULL = '\u0000';

    private final StompFrameAccumulator accumulator = new StompFrameAccumulator();

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static String connectFrame() {
        return "CONNECT\naccept-version:1.2\nheart-beat:10000,10000\n\n" + NULL;
    }

    private static String sendFrame(String body) {
        return "SEND\ndestination:/app/tables/signal\ncontent-type:application/json\n\n"
                + body + NULL;
    }

    @Test
    @DisplayName("decodes a single complete frame")
    void decodesOneFrame() {
        List<Message<byte[]>> frames = accumulator.accumulate(bytes(connectFrame()));

        assertThat(frames).hasSize(1);
        assertThat(StompHeaderAccessor.wrap(frames.get(0)).getCommand())
                .isEqualTo(StompCommand.CONNECT);
        assertThat(accumulator.pendingBytes()).isZero();
    }

    @Test
    @DisplayName("decodes several frames coalesced into one WebSocket message")
    void decodesCoalescedFrames() {
        String coalesced = connectFrame()
                + "SUBSCRIBE\nid:sub-0\ndestination:/topic/tables\n\n" + NULL
                + sendFrame("{\"tableId\":\"T-01\"}");

        List<Message<byte[]>> frames = accumulator.accumulate(bytes(coalesced));

        assertThat(frames).hasSize(3);
        assertThat(frames.stream()
                .map(frame -> StompHeaderAccessor.wrap(frame).getCommand())
                .toList())
                .containsExactly(StompCommand.CONNECT, StompCommand.SUBSCRIBE, StompCommand.SEND);
        assertThat(accumulator.pendingBytes()).isZero();
    }

    @Test
    @DisplayName("carries a partial frame across WebSocket messages and decodes it once complete")
    void reassemblesSplitFrame() {
        String frame = connectFrame();
        int cut = frame.indexOf("heart-beat");

        assertThat(accumulator.accumulate(bytes(frame.substring(0, cut)))).isEmpty();
        assertThat(accumulator.pendingBytes()).isEqualTo(cut);

        List<Message<byte[]>> frames = accumulator.accumulate(bytes(frame.substring(cut)));

        assertThat(frames).hasSize(1);
        assertThat(StompHeaderAccessor.wrap(frames.get(0)).getCommand())
                .isEqualTo(StompCommand.CONNECT);
        assertThat(accumulator.pendingBytes()).isZero();
    }

    @Test
    @DisplayName("reassembles a frame split inside its body")
    void reassemblesSplitInsideBody() {
        String body = "{\"tableId\":\"T-01\",\"signal\":\"GUEST_SEATED\"}";
        String frame = sendFrame(body);
        int cut = frame.indexOf(body) + 10;

        assertThat(accumulator.accumulate(bytes(frame.substring(0, cut)))).isEmpty();
        List<Message<byte[]>> frames = accumulator.accumulate(bytes(frame.substring(cut)));

        assertThat(frames).hasSize(1);
        assertThat(new String(frames.get(0).getPayload(), StandardCharsets.UTF_8)).isEqualTo(body);
    }

    @Test
    @DisplayName("decodes one complete frame and retains the trailing partial one")
    void decodesCompleteAndRetainsPartial() {
        String payload = connectFrame() + "SUBSCRIBE\nid:sub-0\ndestination:/topic/t";

        List<Message<byte[]>> frames = accumulator.accumulate(bytes(payload));

        assertThat(frames).hasSize(1);
        assertThat(accumulator.pendingBytes()).isGreaterThan(0);

        List<Message<byte[]>> completed = accumulator.accumulate(bytes("ables\n\n" + NULL));
        assertThat(completed).hasSize(1);
        assertThat(StompHeaderAccessor.wrap(completed.get(0)).getCommand())
                .isEqualTo(StompCommand.SUBSCRIBE);
    }

    @Test
    @DisplayName("treats a bare end-of-line as a heart-beat rather than a frame")
    void decodesHeartbeat() {
        List<Message<byte[]>> frames = accumulator.accumulate(bytes("\n"));

        assertThat(frames).hasSize(1);
        assertThat(StompHeaderAccessor.wrap(frames.get(0)).getCommand())
                .as("a heart-beat carries no command")
                .isNull();
    }

    @Test
    @DisplayName("decodes an empty payload without producing a frame")
    void toleratesEmptyPayload() {
        assertThat(accumulator.accumulate(new byte[0])).isEmpty();
        assertThat(accumulator.pendingBytes()).isZero();
    }

    @Test
    @DisplayName("refuses an unterminated frame that exceeds the configured ceiling")
    void refusesOversizedPartialFrame() {
        StompFrameAccumulator bounded = new StompFrameAccumulator(64);

        bounded.accumulate(bytes("SEND\ndestination:/app/x\n\n"));

        assertThatExceptionOfType(StompProtocolException.class)
                .isThrownBy(() -> bounded.accumulate(bytes("x".repeat(128))))
                .withMessageContaining("exceeding the 64 byte limit");
        assertThat(bounded.pendingBytes())
                .as("the carried-over buffer is discarded so a hostile client cannot pin memory")
                .isZero();
    }

    @Test
    @DisplayName("refuses a single payload whose retained remainder exceeds the ceiling")
    void refusesOversizedRemainder() {
        StompFrameAccumulator bounded = new StompFrameAccumulator(32);

        assertThatExceptionOfType(StompProtocolException.class)
                .isThrownBy(() -> bounded.accumulate(
                        bytes("SEND\ndestination:/app/tables/signal\n\n" + "y".repeat(64))))
                .withMessageContaining("exceeding the 32 byte limit");
    }

    @Test
    @DisplayName("wraps an undecodable frame as a protocol exception and clears its buffer")
    void wrapsDecodingFailure() {
        assertThatExceptionOfType(StompProtocolException.class)
                .isThrownBy(() -> accumulator.accumulate(
                        bytes("NOT_A_STOMP_COMMAND\nfoo:bar\n\n" + NULL)))
                .withMessageContaining("could not decode");
        assertThat(accumulator.pendingBytes()).isZero();
    }

    @Test
    @DisplayName("resets its carried-over buffer on demand")
    void resetsBuffer() {
        accumulator.accumulate(bytes("SEND\ndestination:/app/x"));
        assertThat(accumulator.pendingBytes()).isGreaterThan(0);

        accumulator.reset();

        assertThat(accumulator.pendingBytes()).isZero();
    }

    @Test
    @DisplayName("defends its construction and call contracts")
    void defendsContracts() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new StompFrameAccumulator(0))
                .withMessageContaining("must be positive");
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new StompFrameAccumulator(-5))
                .withMessageContaining("must be positive");
        assertThatNullPointerException().isThrownBy(() -> accumulator.accumulate(null));
        assertThat(StompFrameAccumulator.DEFAULT_MAX_PARTIAL_FRAME_BYTES).isEqualTo(256 * 1024);
    }
}
