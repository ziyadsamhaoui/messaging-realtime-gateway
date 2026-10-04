package com.ziyadsamhaoui.messagingrealtimegateway.service;

import com.ziyadsamhaoui.messagingrealtimegateway.client.ChatServiceClient;
import com.ziyadsamhaoui.messagingrealtimegateway.dto.Dtos;
import com.ziyadsamhaoui.messagingrealtimegateway.exception.ChatServiceException;
import com.ziyadsamhaoui.messagingrealtimegateway.exception.ErrorCode;
import com.ziyadsamhaoui.messagingrealtimegateway.exception.RateLimitCheckUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class MessageRelayService {

    private static final Logger log = LoggerFactory.getLogger(MessageRelayService.class);

    private final RateLimitService rateLimitService;
    private final ChatServiceClient chatServiceClient;

    public MessageRelayService(RateLimitService rateLimitService,
                               ChatServiceClient chatServiceClient) {
        this.rateLimitService = rateLimitService;
        this.chatServiceClient = chatServiceClient;
    }

    public void relay(String userId, String authorization, Dtos.SendMessageRequest request) {
        RateLimitService.Outcome outcome = rateLimitService.checkMessageSend(userId);
        switch (outcome) {
            case REJECTED -> throw new ChatServiceException(429, ErrorCode.RATE_LIMITED.name(),
                    "Message rate limit exceeded");
            case UNAVAILABLE -> throw new RateLimitCheckUnavailableException("Rate limiter unavailable");
            case ALLOWED -> {

            }
        }

        chatServiceClient.sendMessage(request.roomId(), authorization, request);

        log.debug("Chat accepted message for room {} on behalf of user {}; broadcast follows on {}",
                request.roomId(), userId, "badrlink.chat.message.v1");
    }
}
