package com.ziyadsamhaoui.messagingrealtimegateway.service;

import com.ziyadsamhaoui.messagingrealtimegateway.dto.Dtos;
import com.ziyadsamhaoui.messagingrealtimegateway.messaging.RedisChannels;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

@Service
public class TypingRelayService {

    private static final Logger log = LoggerFactory.getLogger(TypingRelayService.class);

    private final RateLimitService rateLimitService;
    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;

    public TypingRelayService(RateLimitService rateLimitService, StringRedisTemplate redis, ObjectMapper objectMapper) {
        this.rateLimitService = rateLimitService;
        this.redis = redis;
        this.objectMapper = objectMapper;
    }

    public void relay(String userId, Dtos.TypingRequest request) {
        if (rateLimitService.checkTyping(userId) == RateLimitService.Outcome.REJECTED) {
            log.debug("Typing event dropped for user {} (over budget)", userId);
            return;
        }
        try {
            redis.convertAndSend(RedisChannels.typingChannel(request.roomId()),
                    objectMapper.writeValueAsString(new Envelope(userId, request.roomId())));
        } catch (RedisConnectionFailureException e) {
            log.debug("Typing publish failed for room {} (lossy by nature): {}", request.roomId(), e.getMessage());
        }
    }

    public record Envelope(String senderId, String roomId) {
    }
}
