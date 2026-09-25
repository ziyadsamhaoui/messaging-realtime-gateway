package com.ziyadsamhaoui.messagingrealtimegateway.stomp;

import com.ziyadsamhaoui.messagingrealtimegateway.exception.ChatServiceException;
import com.ziyadsamhaoui.messagingrealtimegateway.exception.ErrorCode;
import com.ziyadsamhaoui.messagingrealtimegateway.exception.RateLimitCheckUnavailableException;
import com.ziyadsamhaoui.messagingrealtimegateway.exception.UpstreamUnavailableException;
import org.springframework.messaging.handler.annotation.MessageExceptionHandler;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.web.bind.annotation.ControllerAdvice;

import java.security.Principal;

@ControllerAdvice
public class GlobalStompExceptionHandler {

    private final StompErrorRelay errorRelay;

    public GlobalStompExceptionHandler(StompErrorRelay errorRelay) {
        this.errorRelay = errorRelay;
    }

    @MessageExceptionHandler(ChatServiceException.class)
    public void handleChatRejection(ChatServiceException e, Principal user, SimpMessageHeaderAccessor headers) {
        errorRelay.relay(user, headers.getSessionId(), e.getStatus(), e.getCode(), e.getMessage(),
                headers.getDestination());
    }

    @MessageExceptionHandler(RateLimitCheckUnavailableException.class)
    public void handleRateLimiterDown(RateLimitCheckUnavailableException e, Principal user,
                                      SimpMessageHeaderAccessor headers) {
        errorRelay.relay(user, headers.getSessionId(), 503, ErrorCode.RATE_LIMITER_UNAVAILABLE.name(),
                e.getMessage(), headers.getDestination());
    }

    @MessageExceptionHandler(UpstreamUnavailableException.class)
    public void handleUpstreamDown(UpstreamUnavailableException e, Principal user,
                                   SimpMessageHeaderAccessor headers) {
        errorRelay.relay(user, headers.getSessionId(), 503, ErrorCode.UPSTREAM_UNAVAILABLE.name(),
                e.getMessage(), headers.getDestination());
    }
}
