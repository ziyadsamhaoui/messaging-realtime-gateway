package com.ziyadsamhaoui.messagingrealtimegateway.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class RedisRoomEventSubscriber implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(RedisRoomEventSubscriber.class);

    private final SimpMessagingTemplate messagingTemplate;

    public RedisRoomEventSubscriber(SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    @Override
    public void onMessage(org.springframework.data.redis.connection.Message message, byte[] pattern) {
        String channel = new String(message.getChannel(), StandardCharsets.UTF_8);
        String payload = new String(message.getBody(), StandardCharsets.UTF_8);
        String destination = RedisChannels.destinationForChannel(channel);
        if (destination == null) {
            log.debug("Ignoring pub/sub event on unrecognized channel {}", channel);
            return;
        }
        try {
            messagingTemplate.convertAndSend(destination, payload);
        } catch (Exception e) {
            log.debug("Failed to deliver pub/sub event to destination {}: {}", destination, e.getMessage());
        }
    }
}
