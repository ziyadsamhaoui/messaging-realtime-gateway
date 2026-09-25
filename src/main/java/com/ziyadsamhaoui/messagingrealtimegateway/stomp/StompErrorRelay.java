package com.ziyadsamhaoui.messagingrealtimegateway.stomp;

import tools.jackson.databind.ObjectMapper;
import com.ziyadsamhaoui.messagingrealtimegateway.dto.Dtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.messaging.MessageDeliveryException;
import org.springframework.messaging.simp.SimpMessageType;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.security.Principal;

@Component
public class StompErrorRelay {

    public static final String ERRORS_QUEUE = "/queue/errors";

    private static final Logger log = LoggerFactory.getLogger(StompErrorRelay.class);

    private final org.springframework.messaging.simp.SimpMessagingTemplate messagingTemplate;
    private final ObjectMapper objectMapper;
    private final org.springframework.messaging.MessageChannel clientOutboundChannel;

    public StompErrorRelay(org.springframework.messaging.simp.SimpMessagingTemplate messagingTemplate,
                           ObjectMapper objectMapper,
                           @Qualifier("clientOutboundChannel") org.springframework.messaging.MessageChannel clientOutboundChannel) {
        this.messagingTemplate = messagingTemplate;
        this.objectMapper = objectMapper;
        this.clientOutboundChannel = clientOutboundChannel;
    }

    public void relay(Principal user, String sessionId, int status, String code, String message, String destination) {
        Dtos.ErrorFrame frame = Dtos.ErrorFrame.of(status, code, message, destination);
        if (user != null && user.getName() != null) {
            try {
                messagingTemplate.convertAndSendToUser(user.getName(), ERRORS_QUEUE, frame);
                return;
            } catch (MessageDeliveryException e) {
                log.debug("User-destination delivery failed for session {}; falling back to raw session error", sessionId, e);
            }
        }
        sendRawToSession(sessionId, frame);
    }

    public void sendRawToSession(String sessionId, Dtos.ErrorFrame frame) {
        if (sessionId == null) {
            return;
        }
        try {
            StompHeaderAccessor accessor = StompHeaderAccessor.create(StompCommand.ERROR);
            accessor.setLeaveMutable(true);
            accessor.setMessage(frame.code() + (frame.message() != null ? ": " + frame.message() : ""));
            accessor.setContentType(MediaType.APPLICATION_JSON);
            accessor.setSessionId(sessionId);
            byte[] payload = objectMapper.writeValueAsBytes(frame);
            clientOutboundChannel.send(MessageBuilder.createMessage(payload, accessor.getMessageHeaders()));
        } catch (Exception e) {
            log.debug("Could not deliver error frame to session {} (session likely gone)", sessionId, e);
        }
    }
}
