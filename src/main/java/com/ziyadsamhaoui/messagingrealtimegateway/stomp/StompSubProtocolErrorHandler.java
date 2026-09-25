package com.ziyadsamhaoui.messagingrealtimegateway.stomp;

import com.ziyadsamhaoui.messagingrealtimegateway.dto.Dtos;
import com.ziyadsamhaoui.messagingrealtimegateway.exception.ChatServiceException;
import com.ziyadsamhaoui.messagingrealtimegateway.exception.ErrorCode;
import com.ziyadsamhaoui.messagingrealtimegateway.exception.RateLimitCheckUnavailableException;
import com.ziyadsamhaoui.messagingrealtimegateway.exception.UpstreamUnavailableException;
import org.springframework.http.MediaType;
import org.springframework.messaging.Message;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class StompSubProtocolErrorHandler extends org.springframework.web.socket.messaging.StompSubProtocolErrorHandler {

    private final ObjectMapper objectMapper;

    public StompSubProtocolErrorHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    protected Message<byte[]> handleInternal(StompHeaderAccessor errorHeaderAccessor,
                                             byte[] errorPayload,
                                             Throwable cause,
                                             StompHeaderAccessor clientHeaderAccessor) {
        int status = 500;
        String code = ErrorCode.UNAUTHENTICATED.name();
        String message = "Request failed";
        String destination = clientHeaderAccessor != null ? clientHeaderAccessor.getDestination() : null;

        Throwable effective = cause;
        while (effective != null) {
            if (effective instanceof ChatServiceException e) {
                status = e.getStatus();
                code = e.getCode();
                message = e.getMessage();
                break;
            }
            if (effective instanceof StompAuthException e) {
                status = e.getStatus();
                code = e.getCode().name();
                message = e.getMessage();
                break;
            }
            if (effective instanceof RateLimitCheckUnavailableException) {
                status = 503;
                code = ErrorCode.RATE_LIMITER_UNAVAILABLE.name();
                message = effective.getMessage();
                break;
            }
            if (effective instanceof UpstreamUnavailableException) {
                status = 503;
                code = ErrorCode.UPSTREAM_UNAVAILABLE.name();
                message = effective.getMessage();
                break;
            }
            effective = effective.getCause();
        }

        Dtos.ErrorFrame frame = Dtos.ErrorFrame.of(status, code, message, destination);
        try {
            byte[] body = objectMapper.writeValueAsBytes(frame);
            errorHeaderAccessor.setMessage(code);
            errorHeaderAccessor.setContentType(MediaType.APPLICATION_JSON);
            return MessageBuilder.createMessage(body, errorHeaderAccessor.getMessageHeaders());
        } catch (Exception e) {
            return super.handleInternal(errorHeaderAccessor, errorPayload, cause, clientHeaderAccessor);
        }
    }
}
