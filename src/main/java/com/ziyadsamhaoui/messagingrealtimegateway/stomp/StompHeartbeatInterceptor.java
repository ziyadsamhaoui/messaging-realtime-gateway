package com.ziyadsamhaoui.messagingrealtimegateway.stomp;

import com.ziyadsamhaoui.messagingrealtimegateway.service.PresenceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.stereotype.Component;

import java.security.Principal;

@Component
public class StompHeartbeatInterceptor implements ChannelInterceptor {

    private static final Logger log = LoggerFactory.getLogger(StompHeartbeatInterceptor.class);

    private final PresenceService presenceService;
    private final StompSessionRegistry sessionRegistry;

    public StompHeartbeatInterceptor(PresenceService presenceService, StompSessionRegistry sessionRegistry) {
        this.presenceService = presenceService;
        this.sessionRegistry = sessionRegistry;
    }

    @Override
    public Message<?> preSend(Message<?> message, MessageChannel channel) {
        StompHeaderAccessor accessor = MessageHeaderAccessor.getAccessor(message, StompHeaderAccessor.class);
        if (accessor == null || accessor.getCommand() == null || accessor.getUser() == null) {
            return message;
        }
        Principal user = accessor.getUser();
        if (accessor.getCommand() == StompCommand.SEND) {
            presenceService.onHeartbeat(user.getName());
        }
        return message;
    }
}
