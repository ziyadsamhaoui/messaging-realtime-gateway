package com.ziyadsamhaoui.messagingrealtimegateway.service;

import com.ziyadsamhaoui.messagingrealtimegateway.client.ChatServiceClient;
import com.ziyadsamhaoui.messagingrealtimegateway.dto.Dtos;
import com.ziyadsamhaoui.messagingrealtimegateway.exception.ChatServiceException;
import com.ziyadsamhaoui.messagingrealtimegateway.exception.ErrorCode;
import com.ziyadsamhaoui.messagingrealtimegateway.exception.RateLimitCheckUnavailableException;
import com.ziyadsamhaoui.messagingrealtimegateway.messaging.RedisChannels;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

@Service
public class MessageRelayService {

    private static final Logger log = LoggerFactory.getLogger(MessageRelayService.class);

    private final RateLimitService rateLimitService;
    private final ChatServiceClient chatServiceClient;
    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;

    public MessageRelayService(RateLimitService rateLimitService,
                               ChatServiceClient chatServiceClient,
                               StringRedisTemplate redis,
                               ObjectMapper objectMapper) {
        this.rateLimitService = rateLimitService;
        this.chatServiceClient = chatServiceClient;
        this.redis = redis;
        this.objectMapper = objectMapper;
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

        try {
            redis.convertAndSend(RedisChannels.messagesChannel(request.roomId()),
                    objectMapper.writeValueAsString(new Envelope(userId, request.type(), request.content())));
        } catch (RedisConnectionFailureException e) {

            log.error("Redis publish failed for room {}; message is persisted but will not fan out: {}",
                    request.roomId(), e.getMessage());
        }

        log.debug("Message relayed to room {} on behalf of user {}", request.roomId(), userId);
    }

    public record Envelope(String senderId, String type, String content) {
    }
}
