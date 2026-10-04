package com.ziyadsamhaoui.messagingrealtimegateway.messaging;

import com.ziyadsamhaoui.messagingrealtimegateway.service.MessageFanOutService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Component
@ConditionalOnProperty(prefix = "badrlink.kafka", name = "enabled", havingValue = "true")
public class RealtimeMessageConsumer {

    public static final String MESSAGE_TOPIC = "badrlink.chat.message.v1";
    public static final String ROOM_TOPIC = "badrlink.chat.room.v1";
    public static final String INVITATION_TOPIC = "badrlink.chat.invitation.v1";
    public static final String PROFILE_TOPIC = "badrlink.user.profile.v1";
    public static final String GROUP_ID = "realtime-gateway";

    private static final Logger log = LoggerFactory.getLogger(RealtimeMessageConsumer.class);

    private final MessageFanOutService fanOutService;
    private final ObjectMapper objectMapper;

    public RealtimeMessageConsumer(MessageFanOutService fanOutService, ObjectMapper objectMapper) {
        this.fanOutService = fanOutService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = {MESSAGE_TOPIC, ROOM_TOPIC, INVITATION_TOPIC, PROFILE_TOPIC},
            groupId = GROUP_ID)
    public void onMessage(ConsumerRecord<String, String> record) {
        try {
            JsonNode envelope = objectMapper.readTree(record.value());
            JsonNode eventId = envelope.get("eventId");
            JsonNode eventType = envelope.get("eventType");
            JsonNode payload = envelope.get("payload");
            if (eventId == null || eventId.isNull()
                    || eventType == null || eventType.isNull()
                    || payload == null || payload.isNull()) {
                log.error("Discarding incomplete event from {}: {}", record.topic(), record.value());
                return;
            }
            fanOutService.fanOut(eventId.asString(), eventType.asString(), payload);
        } catch (RuntimeException e) {
            log.error("Failed to process event from {}: {}", record.topic(), record.value(), e);
        }
    }
}
