/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.stomp;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Byte-exact unit suite for the outbound STOMP 1.2 frame writer.</p>
 *
 * <p>These assertions compare whole frames against literal expected strings rather than checking
 * "contains". That is the point of writing the framing explicitly: the bytes a dashboard will
 * receive are pinned here, so a refactor cannot quietly drop a {@code content-length} or reorder a
 * header without a test naming it.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@DisplayName("StompFrameWriter")
class StompFrameWriterTest {

    private static String decode(byte[] frame) {
        return new String(frame, StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("writes a CONNECTED frame terminated by a NULL octet")
    void writesConnected() {
        byte[] frame = StompFrameWriter.connected("1.2", "sess-9", "RestoStream/1.0.0", 10000L, 10000L);

        assertThat(decode(frame)).isEqualTo("""
                CONNECTED
                version:1.2
                session:sess-9
                server:RestoStream/1.0.0
                heart-beat:10000,10000
                content-length:0

                """ + '\u0000');
    }

    @Test
    @DisplayName("writes a MESSAGE frame with destination, subscription, id and content headers")
    void writesMessage() {
        byte[] body = "{\"tableId\":\"T-01\"}".getBytes(StandardCharsets.UTF_8);

        byte[] frame = StompFrameWriter.message(
                "/topic/tables", "sub-0", "sess-9-1", "TABLE_STATE", body);

        assertThat(decode(frame)).isEqualTo("""
                MESSAGE
                destination:/topic/tables
                subscription:sub-0
                message-id:sess-9-1
                content-type:application/json;charset=utf-8
                content-kind:TABLE_STATE
                content-length:18

                {"tableId":"T-01"}""" + '\u0000');
    }

    @Test
    @DisplayName("omits the content-kind header when no classification is supplied")
    void omitsAbsentContentKind() {
        byte[] frame = StompFrameWriter.message("/topic/tables", "sub-0", "m1", null, new byte[0]);

        assertThat(decode(frame)).doesNotContain("content-kind");
        assertThat(decode(frame)).contains("content-length:0");
    }

    @Test
    @DisplayName("writes RECEIPT and ERROR frames")
    void writesReceiptAndError() {
        assertThat(decode(StompFrameWriter.receipt("r-1"))).isEqualTo("""
                RECEIPT
                receipt-id:r-1
                content-length:0

                """ + '\u0000');

        String error = decode(StompFrameWriter.error("not connected", "CONNECT first", "r-2"));
        assertThat(error)
                .startsWith("ERROR\n")
                .contains("message:not connected")
                .contains("content-type:text/plain;charset=utf-8")
                .contains("receipt-id:r-2")
                .contains("content-length:13")
                .endsWith("\n\nCONNECT first" + '\u0000');

        assertThat(decode(StompFrameWriter.error("boom", null, null)))
                .doesNotContain("receipt-id")
                .contains("content-length:0");
    }

    @Test
    @DisplayName("represents a heart-beat as a single end-of-line and returns a fresh array")
    void writesHeartbeat() {
        assertThat(decode(StompFrameWriter.heartbeat())).isEqualTo("\n");
        byte[] first = StompFrameWriter.heartbeat();
        first[0] = 'X';
        assertThat(decode(StompFrameWriter.heartbeat()))
                .as("the shared constant must not be mutable through the accessor")
                .isEqualTo("\n");
    }

    @Test
    @DisplayName("applies STOMP 1.2 header escaping to every reserved octet")
    void escapesReservedOctets() {
        assertThat(StompFrameWriter.escape("plain")).isEqualTo("plain");
        assertThat(StompFrameWriter.escape("a:b")).isEqualTo("a\\cb");
        assertThat(StompFrameWriter.escape("a\\b")).isEqualTo("a\\\\b");
        assertThat(StompFrameWriter.escape("")).isEmpty();
        assertThat(StompFrameWriter.escape("/topic/tables/T-01"))
                .as("ordinary destinations contain nothing that needs escaping")
                .isEqualTo("/topic/tables/T-01");
    }

    @Test
    @DisplayName("escapes carriage returns and newlines in header values")
    void escapesControlCharacters() {
        assertThat(StompFrameWriter.escape("a\nb")).isEqualTo("a\\nb");
        assertThat(StompFrameWriter.escape("a\rb")).isEqualTo("a\\rb");
        assertThatNullPointerException().isThrownBy(() -> StompFrameWriter.escape(null));
    }

    @Test
    @DisplayName("prevents a client-supplied receipt value from forging an extra header")
    void escapingPreventsHeaderInjection() {
        String hostile = "r1\nmessage:spoofed";

        String frame = decode(StompFrameWriter.receipt(hostile));

        assertThat(frame).contains("receipt-id:r1\\nmessage\\cspoofed");
        assertThat(frame.lines().filter(line -> line.startsWith("message:")).count())
                .as("the injected header must not appear as a real header")
                .isZero();
    }

    @Test
    @DisplayName("assembles an arbitrary frame and rejects null arguments")
    void assemblesArbitraryFrames() {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("alpha", "1");
        headers.put("beta", "2");

        assertThat(decode(StompFrameWriter.frame("PING", headers, null))).isEqualTo("""
                PING
                alpha:1
                beta:2
                content-length:0

                """ + '\u0000');

        assertThatNullPointerException()
                .isThrownBy(() -> StompFrameWriter.frame(null, headers, null));
        assertThatNullPointerException()
                .isThrownBy(() -> StompFrameWriter.frame("PING", null, null));
        assertThatNullPointerException()
                .isThrownBy(() -> StompFrameWriter.message("/d", "s", "m", null, null));
    }

    @Test
    @DisplayName("is a utility class that cannot be instantiated")
    void cannotBeInstantiated() throws Exception {
        var constructor = StompFrameWriter.class.getDeclaredConstructor();
        constructor.setAccessible(true);
        assertThatExceptionOfType(java.lang.reflect.InvocationTargetException.class)
                .isThrownBy(constructor::newInstance)
                .withCauseInstanceOf(AssertionError.class);
    }
}
