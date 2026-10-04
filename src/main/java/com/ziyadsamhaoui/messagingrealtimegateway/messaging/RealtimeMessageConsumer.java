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

    public static final String TOPIC = "badrlink.chat.message.v1";
    public static final String GROUP_ID = "realtime-gateway";
    public static final String MESSAGE_SENT = "MESSAGE_SENT";

    private static final Logger log = LoggerFactory.getLogger(RealtimeMessageConsumer.class);

    private final MessageFanOutService fanOutService;
    private final ObjectMapper objectMapper;

    public RealtimeMessageConsumer(MessageFanOutService fanOutService, ObjectMapper objectMapper) {
        this.fanOutService = fanOutService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = TOPIC, groupId = GROUP_ID)
    public void onMessage(ConsumerRecord<String, String> record) {
        try {
            JsonNode envelope = objectMapper.readTree(record.value());
            JsonNode eventType = envelope.get("eventType");
            if (eventType == null || !MESSAGE_SENT.equals(eventType.asString())) {
                log.debug("Ignoring non-{} event on {}", MESSAGE_SENT, TOPIC);
                return;
            }
            JsonNode payload = envelope.get("payload");
            if (payload == null || payload.isNull()) {
                log.error("{} on {} carried no payload", MESSAGE_SENT, TOPIC);
                return;
            }
            fanOutService.fanOut(
                    text(payload, "messageId"),
                    text(payload, "roomId"),
                    text(payload, "senderId"),
                    text(payload, "type"),
                    text(payload, "content"));
        } catch (RuntimeException e) {
            log.error("Failed to process event from {}: {}", TOPIC, record.value(), e);
        }
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asString();
    }
}
