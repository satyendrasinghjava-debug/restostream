/*
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 */
package com.satyendra.restostream.config;

import com.satyendra.restostream.stomp.ReactiveStompWebSocketHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;

import java.util.Map;

/**
 * © 2026 Satyendra Singh. All Rights Reserved.
 * Author: Satyendra Singh
 *
 * <p>Publishes the reactive STOMP endpoint.</p>
 *
 * <p>There is no {@code @EnableWebSocketMessageBroker} here, and deliberately so: that annotation
 * belongs to the Servlet stack and would pull a thread-per-connection broker into a service whose
 * entire premise is a non-blocking one. On WebFlux the equivalent is a single
 * {@link SimpleUrlHandlerMapping} binding a path to a {@code WebSocketHandler}; the handshake,
 * upgrade and frame plumbing are supplied by the auto-configured
 * {@code WebSocketHandlerAdapter} and the Reactor Netty upgrade strategy.</p>
 *
 * <p>The mapping is ordered ahead of the annotated-controller mappings so that the WebSocket path
 * is never shadowed by a request mapping, which is the usual cause of a handshake mysteriously
 * returning {@code 404} or {@code 405}.</p>
 *
 * @author Satyendra Singh
 * @since 1.0.0
 */
@Configuration(proxyBeanMethods = false)
public class WebSocketConfig {

    /**
     * Binds the STOMP broker to its configured path.
     *
     * @param handler    the reactive STOMP broker
     * @param properties application configuration supplying the endpoint path
     * @return the handler mapping
     */
    @Bean
    public HandlerMapping stompWebSocketHandlerMapping(ReactiveStompWebSocketHandler handler,
                                                       RestoStreamProperties properties) {
        SimpleUrlHandlerMapping mapping = new SimpleUrlHandlerMapping();
        mapping.setUrlMap(Map.of(properties.stomp().endpoint(), handler));
        mapping.setOrder(Ordered.HIGHEST_PRECEDENCE + 100);
        return mapping;
    }
}
