package com.ziyadsamhaoui.messagingrealtimegateway.stomp;

import com.ziyadsamhaoui.messagingrealtimegateway.dto.Dtos;
import com.ziyadsamhaoui.messagingrealtimegateway.exception.ErrorCode;
import com.ziyadsamhaoui.messagingrealtimegateway.service.MessageRelayService;
import com.ziyadsamhaoui.messagingrealtimegateway.service.TypingRelayService;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.stereotype.Controller;

import java.security.Principal;

@Controller
public class StompSendController {

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
        String token = (principal instanceof JwtPrincipal jwtPrincipal) ? jwtPrincipal.tokenValue() : null;

        String authorization = token != null ? "Bearer " + token : null;
        messageRelayService.relay(principal.getName(), authorization, request);
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
