package com.ziyadsamhaoui.messagingrealtimegateway.stomp;

import com.ziyadsamhaoui.messagingrealtimegateway.client.ChatServiceClient;
import com.ziyadsamhaoui.messagingrealtimegateway.exception.ChatServiceException;
import com.ziyadsamhaoui.messagingrealtimegateway.exception.UpstreamUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.stereotype.Component;

import java.security.Principal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class StompSubscriptionInterceptor implements ChannelInterceptor {

    static final Pattern ROOM_TOPIC_PATTERN = Pattern.compile("^/topic/rooms/([^/]+)(/typing)?$");

    private static final Logger log = LoggerFactory.getLogger(StompSubscriptionInterceptor.class);

    private final ChatServiceClient chatServiceClient;

    public StompSubscriptionInterceptor(ChatServiceClient chatServiceClient) {
        this.chatServiceClient = chatServiceClient;
    }

    @Override
    public Message<?> preSend(Message<?> message, MessageChannel channel) {
        StompHeaderAccessor accessor = MessageHeaderAccessor.getAccessor(message, StompHeaderAccessor.class);
        if (accessor == null || accessor.getCommand() != StompCommand.SUBSCRIBE) {
            return message;
        }

        String destination = accessor.getDestination();
        Principal user = accessor.getUser();
        if (user == null || user.getName() == null) {
            throw new StompAuthException(401, com.ziyadsamhaoui.messagingrealtimegateway.exception.ErrorCode.UNAUTHENTICATED,
                    "No authenticated session; CONNECT must succeed before subscribing");
        }

        if (destination != null && destination.startsWith("/user/")) {
            return message;
        }

        if (destination == null || !destination.startsWith("/topic/rooms/")) {

            log.warn("Subscription denied (session {}): destination {} is not a subscribable topic",
                    accessor.getSessionId(), destination);
            throw new ChatServiceException(403, "ROOM_ACCESS_DENIED",
                    "Destination is not a subscribable topic: " + destination);
        }

        Matcher matcher = ROOM_TOPIC_PATTERN.matcher(destination);
        if (!matcher.matches()) {
            log.warn("Subscription denied (session {}): malformed room destination {}", accessor.getSessionId(), destination);
            throw new ChatServiceException(403, "ROOM_ACCESS_DENIED",
                    "Destination is not a subscribable topic: " + destination);
        }
        String roomId = matcher.group(1);

        String authorization = bearerToken(user);
        if (authorization == null) {
            throw new StompAuthException(401, com.ziyadsamhaoui.messagingrealtimegateway.exception.ErrorCode.UNAUTHENTICATED,
                    "No credentials available for subscription");
        }

        try {
            chatServiceClient.getRoom(roomId, authorization);
        } catch (ChatServiceException | UpstreamUnavailableException e) {

            log.warn("Subscription denied (session {}, user {}, destination {}): {}",
                    accessor.getSessionId(), user.getName(), destination, e.getMessage());
            throw e;
        }

        log.debug("Subscription allowed (session {}, user {}, destination {})",
                accessor.getSessionId(), user.getName(), destination);
        return message;
    }

    private String bearerToken(Principal user) {
        if (user instanceof JwtPrincipal jwtPrincipal) {
            return "Bearer " + jwtPrincipal.tokenValue();
        }
        return null;
    }
}
