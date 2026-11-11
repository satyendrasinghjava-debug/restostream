/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.stomp;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Raised when a client violates the STOMP 1.2 protocol — an unparseable frame, a frame sent
 * before {@code CONNECT}, a {@code SUBSCRIBE} without the mandatory headers, or a partial frame that
 * exceeds the configured buffer ceiling. The broker answers with an {@code ERROR} frame and closes
 * the session, as the specification requires.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
public class StompProtocolException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * @param message operator- and client-facing explanation
     */
    public StompProtocolException(String message) {
        super(message);
    }

    /**
     * @param message operator- and client-facing explanation
     * @param cause   the underlying decoding failure
     */
    public StompProtocolException(String message, Throwable cause) {
        super(message, cause);
    }
}
