package com.ziyadsamhaoui.messagingrealtimegateway.stomp;

import com.ziyadsamhaoui.messagingrealtimegateway.dto.Dtos;
import com.ziyadsamhaoui.messagingrealtimegateway.exception.ErrorCode;
import com.ziyadsamhaoui.messagingrealtimegateway.service.MessageRelayService;
import com.ziyadsamhaoui.messagingrealtimegateway.service.TypingRelayService;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Controller;

import java.security.Principal;

@Controller
public class StompSendController {

    private static final Tracer TRACER = GlobalOpenTelemetry.get()
            .getTracer("com.ziyadsamhaoui.messagingrealtimegateway");

    private final MessageRelayService messageRelayService;
    private final TypingRelayService typingRelayService;

    public StompSendController(MessageRelayService messageRelayService,
                               TypingRelayService typingRelayService) {
        this.messageRelayService = messageRelayService;
        this.typingRelayService = typingRelayService;
    }

    @MessageMapping("/chat.sendMessage")
    public void sendMessage(@Payload Dtos.SendMessageRequest request, Principal principal) {
        requireAuthenticated(principal);

        Span span = TRACER.spanBuilder("stomp.send")
                .setAttribute("messaging.system", "stomp")
                .setAttribute("messaging.destination.name", "/chat.sendMessage")
                .setAttribute("messaging.operation", "send")
                .setAttribute("chat.room.id", request.roomId())
                .startSpan();
        try (Scope scope = span.makeCurrent()) {
            String token = (principal instanceof JwtPrincipal jwtPrincipal) ? jwtPrincipal.tokenValue() : null;
            String authorization = token != null ? "Bearer " + token : null;
            messageRelayService.relay(principal.getName(), authorization, request);
        } catch (RuntimeException exception) {
            span.recordException(exception);
            span.setStatus(StatusCode.ERROR, String.valueOf(exception.getMessage()));
            throw exception;
        } finally {
            span.end();
        }
    }

    @MessageMapping("/chat.typing")
    public void sendTyping(@Payload Dtos.TypingRequest request, Principal principal) {
        requireAuthenticated(principal);
        typingRelayService.relay(principal.getName(), request);
    }

    private static void requireAuthenticated(Principal principal) {
        if (principal == null || principal.getName() == null) {
            throw new StompAuthException(401, ErrorCode.UNAUTHENTICATED,
                    "No authenticated session; CONNECT must succeed before sending");
        }
    }
}
