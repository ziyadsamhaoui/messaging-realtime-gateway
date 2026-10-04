package com.ziyadsamhaoui.messagingrealtimegateway;

import com.ziyadsamhaoui.messagingrealtimegateway.service.MessageFanOutService;
import com.ziyadsamhaoui.messagingrealtimegateway.support.AbstractIntegrationTest;
import com.ziyadsamhaoui.messagingrealtimegateway.support.StompTestClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class MessageFanOutIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    MessageFanOutService fanOutService;

    @Autowired
    StringRedisTemplate redis;

    private StompTestClient connectedSubscriber(String userId) throws InterruptedException {
        StompTestClient client = new StompTestClient();
        String token = TOKENS.mint(userId);
        boolean ok = client.connect("ws://localhost:" + port() + "/ws/websocket",
                h -> h.add("Authorization", "Bearer " + token), 5);
        if (!ok) {
            client.close();
            throw new IllegalStateException("client failed to connect: " + client.connectError());
        }
        client.subscribe("/topic/rooms/42");
        Thread.sleep(250);
        return client;
    }

    @Test
    void messageSentEventBroadcastsToRoomSubscribers() throws Exception {
        try (StompTestClient subscriber = connectedSubscriber("viewer-1")) {
            fanOutService.fanOut("fanout-basic", "42", "sender-1", "TEXT", "hello");

            String frame = subscriber.nextFrame("/topic/rooms/42", 5);
            assertThat(frame).isNotNull();
            assertThat(frame).contains("\"senderId\":\"sender-1\"");
            assertThat(frame).contains("\"content\":\"hello\"");
        }
    }

    @Test
    void duplicateEventBroadcastsExactlyOnce() throws Exception {
        try (StompTestClient subscriber = connectedSubscriber("viewer-2")) {
            fanOutService.fanOut("fanout-dup", "42", "sender-1", "TEXT", "once");
            fanOutService.fanOut("fanout-dup", "42", "sender-1", "TEXT", "once");

            assertThat(subscriber.nextFrame("/topic/rooms/42", 5)).isNotNull();
            assertThat(subscriber.nextFrame("/topic/rooms/42", 1)).isNull();
        }
    }

    @Test
    void deduplicationKeyIsClaimedWithTtl() {
        fanOutService.fanOut("fanout-ttl", "42", "sender-1", "TEXT", "x");

        assertThat(redis.hasKey("message_dedup:fanout-ttl")).isTrue();
        Long ttl = redis.getExpire("message_dedup:fanout-ttl", TimeUnit.SECONDS);
        assertThat(ttl).isPositive();
        assertThat(ttl).isLessThanOrEqualTo(60);
    }
}
