package com.ziyadsamhaoui.messagingrealtimegateway.stomp;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.simp.config.ChannelRegistration;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

@Configuration
public class StompChannelConfig implements WebSocketMessageBrokerConfigurer {

    private final StompAuthChannelInterceptor authInterceptor;
    private final StompSubscriptionInterceptor subscriptionInterceptor;
    private final StompHeartbeatInterceptor heartbeatInterceptor;
    private final StompSessionRegistry sessionRegistry;

    public StompChannelConfig(StompAuthChannelInterceptor authInterceptor,
                              StompSubscriptionInterceptor subscriptionInterceptor,
                              StompHeartbeatInterceptor heartbeatInterceptor,
                              StompSessionRegistry sessionRegistry) {
        this.authInterceptor = authInterceptor;
        this.subscriptionInterceptor = subscriptionInterceptor;
        this.heartbeatInterceptor = heartbeatInterceptor;
        this.sessionRegistry = sessionRegistry;
    }

    @Override
    public void configureClientInboundChannel(ChannelRegistration registration) {
        registration.interceptors(authInterceptor, subscriptionInterceptor, heartbeatInterceptor,
                new ChannelInterceptor() {
                    @Override
                    public void postSend(Message<?> message, MessageChannel channel, boolean sent) {
                        StompHeaderAccessor accessor = MessageHeaderAccessor.getAccessor(message, StompHeaderAccessor.class);
                        if (accessor == null || accessor.getCommand() == null) {
                            return;
                        }
                        String sessionId = accessor.getSessionId();
                        if (sessionId == null) {
                            return;
                        }
                        if (accessor.getCommand() == StompCommand.DISCONNECT) {
                            sessionRegistry.unregister(sessionId);
                        }
                    }
                });
    }
}
