package com.ziyadsamhaoui.messagingrealtimegateway.config;

import com.ziyadsamhaoui.messagingrealtimegateway.stomp.StompSubProtocolErrorHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

    private final long heartbeatIntervalMs;
    private final StompSubProtocolErrorHandler subProtocolErrorHandler;

    public WebSocketConfig(@Value("${realtime.heartbeat-interval-ms}") long heartbeatIntervalMs,
                           StompSubProtocolErrorHandler subProtocolErrorHandler) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.subProtocolErrorHandler = subProtocolErrorHandler;
    }

    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {

        registry.setErrorHandler(subProtocolErrorHandler);

        registry.addEndpoint("/ws")
                .setAllowedOriginPatterns("*");

        registry.addEndpoint("/ws")
                .setAllowedOriginPatterns("*")
                .withSockJS();
    }

    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {

        registry.enableSimpleBroker("/topic", "/queue");
        registry.setApplicationDestinationPrefixes("/app");
        registry.setUserDestinationPrefix("/user");
    }

    long heartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }
}
