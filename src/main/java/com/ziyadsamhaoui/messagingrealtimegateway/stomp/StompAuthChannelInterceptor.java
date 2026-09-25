package com.ziyadsamhaoui.messagingrealtimegateway.stomp;

import com.ziyadsamhaoui.messagingrealtimegateway.exception.ErrorCode;
import com.ziyadsamhaoui.messagingrealtimegateway.service.PresenceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.stereotype.Component;

import java.security.Principal;
import java.util.List;

@Component
public class StompAuthChannelInterceptor implements ChannelInterceptor {

    private static final Logger log = LoggerFactory.getLogger(StompAuthChannelInterceptor.class);

    private final JwtDecoder jwtDecoder;
    private final PresenceService presenceService;
    private final StompSessionRegistry sessionRegistry;

    public StompAuthChannelInterceptor(JwtDecoder jwtDecoder,
                                       PresenceService presenceService,
                                       StompSessionRegistry sessionRegistry) {
        this.jwtDecoder = jwtDecoder;
        this.presenceService = presenceService;
        this.sessionRegistry = sessionRegistry;
    }

    @Override
    public Message<?> preSend(Message<?> message, MessageChannel channel) {
        StompHeaderAccessor accessor = MessageHeaderAccessor.getAccessor(message, StompHeaderAccessor.class);
        if (accessor == null || accessor.getCommand() == null) {
            return message;
        }

        if (accessor.getCommand() == StompCommand.CONNECT) {
            return handleConnect(message, accessor);
        }
        return message;
    }

    private Message<?> handleConnect(Message<?> message, StompHeaderAccessor accessor) {
        String authorization = accessor.getFirstNativeHeader("Authorization");
        if (authorization == null || authorization.isBlank()) {
            log.warn("Rejected CONNECT (session {}): missing Authorization header", accessor.getSessionId());
            throw new StompAuthException(401, ErrorCode.UNAUTHENTICATED, "Missing Authorization header on CONNECT");
        }

        String token = authorization.startsWith("Bearer ")
                ? authorization.substring("Bearer ".length()).trim()
                : authorization.trim();

        Jwt jwt;
        try {
            jwt = jwtDecoder.decode(token);
        } catch (org.springframework.security.oauth2.jwt.BadJwtException e) {

            log.warn("Rejected CONNECT (session {}): invalid or expired JWT", accessor.getSessionId());
            throw new StompAuthException(401, ErrorCode.UNAUTHENTICATED, "Invalid or expired token");
        } catch (Exception e) {

            log.warn("Rejected CONNECT (session {}): token validation unavailable: {}",
                    accessor.getSessionId(), e.toString());
            throw new StompAuthException(503, ErrorCode.UPSTREAM_UNAVAILABLE, "Authentication backend unavailable");
        }

        String subject = jwt.getSubject();
        if (subject == null || subject.isBlank()) {
            log.debug("Rejected CONNECT (session {}): token has no sub claim", accessor.getSessionId());
            throw new StompAuthException(401, ErrorCode.UNAUTHENTICATED, "Token has no subject");
        }

        Principal principal = new JwtPrincipal(jwt);
        accessor.setUser(principal);

        sessionRegistry.register(accessor.getSessionId(), principal);
        presenceService.onConnect(subject);

        log.debug("CONNECT accepted (session {}, user {})", accessor.getSessionId(), subject);
        return message;
    }
}
