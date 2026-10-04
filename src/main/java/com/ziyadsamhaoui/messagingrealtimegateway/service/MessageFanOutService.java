package com.ziyadsamhaoui.messagingrealtimegateway.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;

@Service
public class MessageFanOutService {

    public static final String DEDUP_KEY_PREFIX = "message_dedup:";
    public static final Duration DEDUP_TTL = Duration.ofSeconds(60);
    public static final String ROOM_TOPIC_PREFIX = "/topic/rooms/";

    private static final Logger log = LoggerFactory.getLogger(MessageFanOutService.class);

    private final StringRedisTemplate redis;
    private final SimpMessagingTemplate messagingTemplate;
    private final ObjectMapper objectMapper;

    public MessageFanOutService(StringRedisTemplate redis,
                                SimpMessagingTemplate messagingTemplate,
                                ObjectMapper objectMapper) {
        this.redis = redis;
        this.messagingTemplate = messagingTemplate;
        this.objectMapper = objectMapper;
    }

    public void fanOut(String messageId, String roomId, String senderId, String type, String content) {
        if (messageId == null || messageId.isBlank() || roomId == null || roomId.isBlank()) {
            log.error("Discarding MESSAGE_SENT with missing messageId or roomId (messageId={}, roomId={})",
                    messageId, roomId);
            return;
        }
        if (!claim(messageId)) {
            log.debug("Duplicate MESSAGE_SENT {} skipped; another node or execution already broadcast it",
                    messageId);
            return;
        }
        String destination = ROOM_TOPIC_PREFIX + roomId;
        try {
            String frame = objectMapper.writeValueAsString(new RoomMessageFrame(senderId, type, content));
            messagingTemplate.convertAndSend(destination, frame);
            log.debug("Broadcast message {} to {} on behalf of {}", messageId, destination, senderId);
        } catch (Exception e) {
            log.error("Failed to broadcast message {} to {}: {}", messageId, destination, e.getMessage());
        }
    }

    private boolean claim(String messageId) {
        try {
            Boolean first = redis.opsForValue().setIfAbsent(DEDUP_KEY_PREFIX + messageId, "1", DEDUP_TTL);
            return Boolean.TRUE.equals(first);
        } catch (DataAccessException e) {
            log.error("Deduplication unavailable for message {}; broadcasting without deduplication: {}",
                    messageId, e.getMessage());
            return true;
        }
    }

    public record RoomMessageFrame(String senderId, String type, String content) {
    }
}
