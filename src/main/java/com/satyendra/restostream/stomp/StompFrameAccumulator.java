/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.stomp;

import org.springframework.messaging.Message;
import org.springframework.messaging.simp.stomp.StompDecoder;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Reassembles STOMP frames from WebSocket message chunks.</p>
 *
 * <p>This class exists because of a detail that is easy to get wrong: a WebSocket message boundary
 * is <strong>not</strong> a STOMP frame boundary. The protocols are independent, so a conforming
 * client may split one frame across two WebSocket messages, or pack three frames into one. A broker
 * that assumes one message equals one frame works perfectly against the common JavaScript client
 * and then corrupts traffic the first time a large payload is fragmented or a client coalesces its
 * writes.</p>
 *
 * <p>{@link StompDecoder} already handles both cases correctly: it decodes as many complete frames
 * as the buffer holds and rewinds the buffer's position to the start of any trailing partial frame.
 * This accumulator supplies it with the carried-over bytes and retains whatever is left, which is
 * why Spring's own production decoder is reused rather than a hand-rolled parser.</p>
 *
 * <p>Instances are confined to a single session and all access is synchronised, since a session's
 * inbound frames are processed sequentially but not necessarily on the same event-loop thread.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public final class StompFrameAccumulator {

    /** Default ceiling on a single partially-received frame. */
    public static final int DEFAULT_MAX_PARTIAL_FRAME_BYTES = 256 * 1024;

    private static final byte[] EMPTY = new byte[0];

    private final StompDecoder decoder = new StompDecoder();
    private final int maxPartialFrameBytes;

    private byte[] pending = EMPTY;

    /**
     * Creates an accumulator with the default partial-frame ceiling.
     */
    public StompFrameAccumulator() {
        this(DEFAULT_MAX_PARTIAL_FRAME_BYTES);
    }

    /**
     * Creates an accumulator.
     *
     * @param maxPartialFrameBytes ceiling on the carried-over bytes of one incomplete frame; must be
     *                             positive. A client that exceeds it is disconnected rather than
     *                             allowed to exhaust broker memory with an unterminated frame.
     */
    public StompFrameAccumulator(int maxPartialFrameBytes) {
        if (maxPartialFrameBytes <= 0) {
            throw new IllegalArgumentException(
                    "maxPartialFrameBytes must be positive but was " + maxPartialFrameBytes);
        }
        this.maxPartialFrameBytes = maxPartialFrameBytes;
    }

    /**
     * Appends a WebSocket payload and decodes every complete STOMP frame now available.
     *
     * @param chunk the raw bytes of one WebSocket message; must not be {@code null}
     * @return the frames decoded by this call, in order; empty when the frame is still incomplete
     * @throws StompProtocolException when the carried-over partial frame exceeds the ceiling, or the
     *                                bytes are not valid STOMP
     */
    public synchronized List<Message<byte[]>> accumulate(byte[] chunk) {
        Objects.requireNonNull(chunk, "chunk must not be null");
        ByteBuffer buffer = combine(chunk);
        List<Message<byte[]>> frames;
        try {
            frames = decoder.decode(buffer);
        } catch (RuntimeException ex) {
            pending = EMPTY;
            throw new StompProtocolException("could not decode the inbound STOMP frame", ex);
        }
        retain(buffer);
        return frames;
    }

    /**
     * @return the number of bytes currently carried over from an incomplete frame
     */
    public synchronized int pendingBytes() {
        return pending.length;
    }

    /**
     * Discards any carried-over bytes. Invoked when a session is torn down.
     */
    public synchronized void reset() {
        pending = EMPTY;
    }

    private ByteBuffer combine(byte[] chunk) {
        if (pending.length == 0) {
            return ByteBuffer.wrap(chunk);
        }
        int total = pending.length + chunk.length;
        if (total > maxPartialFrameBytes) {
            pending = EMPTY;
            throw new StompProtocolException(
                    "an incomplete STOMP frame reached %d bytes, exceeding the %d byte limit"
                            .formatted(total, maxPartialFrameBytes));
        }
        byte[] combined = new byte[total];
        System.arraycopy(pending, 0, combined, 0, pending.length);
        System.arraycopy(chunk, 0, combined, pending.length, chunk.length);
        return ByteBuffer.wrap(combined);
    }

    private void retain(ByteBuffer buffer) {
        int remaining = buffer.remaining();
        if (remaining == 0) {
            pending = EMPTY;
            return;
        }
        if (remaining > maxPartialFrameBytes) {
            pending = EMPTY;
            throw new StompProtocolException(
                    "an incomplete STOMP frame reached %d bytes, exceeding the %d byte limit"
                            .formatted(remaining, maxPartialFrameBytes));
        }
        byte[] leftover = new byte[remaining];
        buffer.get(leftover);
        pending = leftover;
    }
}
