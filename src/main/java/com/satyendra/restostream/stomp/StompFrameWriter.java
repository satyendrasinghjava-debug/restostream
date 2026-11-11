/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.stomp;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Byte-exact writer for the four server-to-client STOMP 1.2 frames RestoStream emits:
 * {@code CONNECTED}, {@code MESSAGE}, {@code RECEIPT} and {@code ERROR}.</p>
 *
 * <h2>Why outbound framing is written here rather than delegated</h2>
 * <p>Inbound parsing is delegated to Spring's {@link org.springframework.messaging.simp.stomp.StompDecoder},
 * because parsing is the hard, adversarial direction and that decoder is production-hardened.
 * Outbound framing is the opposite: the grammar is a dozen lines, it is fully specified, and writing
 * it explicitly means the exact bytes on the wire are visible in one file and can be asserted
 * byte-for-byte in a unit test. Routing a server frame through the Servlet-oriented header-accessor
 * machinery instead would couple the reactive broker to conversion internals for no benefit.</p>
 *
 * <h2>Header escaping</h2>
 * <p>STOMP 1.2 requires the octets {@code \\}, {@code :}, {@code \r} and {@code \n} to be escaped
 * inside header names and values. RestoStream's own destinations and identifiers never contain them,
 * but a client-supplied {@code receipt} or correlation value can, and an unescaped colon would let a
 * client inject a forged header into a frame addressed to another subscriber. Escaping is therefore
 * applied unconditionally.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public final class StompFrameWriter {

    /** Frame terminator octet mandated by the specification. */
    private static final byte NULL_OCTET = 0x00;

    /** A bare end-of-line, which is the STOMP representation of a heart-beat. */
    private static final byte[] HEARTBEAT = "\n".getBytes(StandardCharsets.UTF_8);

    /** Non-standard header used to classify a payload so dashboards need not sniff the JSON. */
    public static final String CONTENT_KIND_HEADER = "content-kind";

    private StompFrameWriter() {
        throw new AssertionError("StompFrameWriter is a utility class and must not be instantiated");
    }

    /**
     * Builds the {@code CONNECTED} frame that completes the STOMP handshake.
     *
     * @param version          the negotiated protocol version, e.g. {@code 1.2}
     * @param sessionId         the broker's session identifier
     * @param serverName        the {@code server} header value
     * @param heartbeatSendMs   milliseconds between heart-beats the server promises to send
     * @param heartbeatExpectMs milliseconds between heart-beats the server expects to receive
     * @return the encoded frame
     */
    public static byte[] connected(String version,
                                   String sessionId,
                                   String serverName,
                                   long heartbeatSendMs,
                                   long heartbeatExpectMs) {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("version", version);
        headers.put("session", sessionId);
        headers.put("server", serverName);
        headers.put("heart-beat", heartbeatSendMs + "," + heartbeatExpectMs);
        return frame("CONNECTED", headers, null);
    }

    /**
     * Builds a {@code MESSAGE} frame delivering a JSON payload to one subscription.
     *
     * @param destination    the destination the subscription was made against
     * @param subscriptionId the client's subscription identifier
     * @param messageId      a per-session unique message identifier
     * @param contentKind    value of the {@value #CONTENT_KIND_HEADER} header; may be {@code null}
     * @param body           the UTF-8 JSON body; must not be {@code null}
     * @return the encoded frame
     */
    public static byte[] message(String destination,
                                 String subscriptionId,
                                 String messageId,
                                 String contentKind,
                                 byte[] body) {
        Objects.requireNonNull(body, "body must not be null");
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("destination", destination);
        headers.put("subscription", subscriptionId);
        headers.put("message-id", messageId);
        headers.put("content-type", "application/json;charset=utf-8");
        if (contentKind != null) {
            headers.put(CONTENT_KIND_HEADER, contentKind);
        }
        return frame("MESSAGE", headers, body);
    }

    /**
     * Builds a {@code RECEIPT} frame acknowledging a client frame that carried a {@code receipt}
     * header.
     *
     * @param receiptId the value of the client's {@code receipt} header
     * @return the encoded frame
     */
    public static byte[] receipt(String receiptId) {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("receipt-id", receiptId);
        return frame("RECEIPT", headers, null);
    }

    /**
     * Builds an {@code ERROR} frame. Per the specification the broker closes the session
     * immediately after sending one.
     *
     * @param shortDescription the {@code message} header, a one-line summary
     * @param detail           the frame body carrying the full explanation; may be {@code null}
     * @param receiptId        the client {@code receipt} this error answers; may be {@code null}
     * @return the encoded frame
     */
    public static byte[] error(String shortDescription, String detail, String receiptId) {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("message", shortDescription);
        headers.put("content-type", "text/plain;charset=utf-8");
        if (receiptId != null) {
            headers.put("receipt-id", receiptId);
        }
        byte[] body = detail == null ? null : detail.getBytes(StandardCharsets.UTF_8);
        return frame("ERROR", headers, body);
    }

    /**
     * @return the heart-beat payload: a single end-of-line octet sequence
     */
    public static byte[] heartbeat() {
        return HEARTBEAT.clone();
    }

    /**
     * Assembles an arbitrary frame. Exposed for the unit suite, which asserts the exact bytes.
     *
     * @param command the STOMP command
     * @param headers the headers, written in iteration order
     * @param body    the body, or {@code null} for a body-less frame
     * @return the encoded frame
     */
    public static byte[] frame(String command, Map<String, String> headers, byte[] body) {
        Objects.requireNonNull(command, "command must not be null");
        Objects.requireNonNull(headers, "headers must not be null");
        byte[] payload = body == null ? new byte[0] : body;
        ByteArrayOutputStream out = new ByteArrayOutputStream(128 + payload.length);
        writeAscii(out, command);
        writeAscii(out, "\n");
        headers.forEach((name, value) -> {
            writeAscii(out, escape(name));
            writeAscii(out, ":");
            writeAscii(out, escape(value));
            writeAscii(out, "\n");
        });
        // content-length is mandatory for us: it is what lets a client read a body containing a
        // NULL octet, and it removes any ambiguity about where the frame ends.
        writeAscii(out, "content-length:" + payload.length + "\n");
        writeAscii(out, "\n");
        out.write(payload, 0, payload.length);
        out.write(NULL_OCTET);
        return out.toByteArray();
    }

    /**
     * Applies STOMP 1.2 header escaping.
     *
     * @param raw the unescaped header name or value; must not be {@code null}
     * @return the escaped form
     */
    public static String escape(String raw) {
        Objects.requireNonNull(raw, "raw must not be null");
        StringBuilder escaped = new StringBuilder(raw.length() + 8);
        for (int index = 0; index < raw.length(); index++) {
            char current = raw.charAt(index);
            switch (current) {
                case '\\' -> escaped.append("\\\\");
                case ':' -> escaped.append("\\c");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                default -> escaped.append(current);
            }
        }
        return escaped.toString();
    }

    private static void writeAscii(ByteArrayOutputStream out, String text) {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        out.write(bytes, 0, bytes.length);
    }
}
