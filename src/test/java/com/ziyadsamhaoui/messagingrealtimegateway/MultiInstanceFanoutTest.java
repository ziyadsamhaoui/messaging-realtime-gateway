package com.ziyadsamhaoui.messagingrealtimegateway;

import com.ziyadsamhaoui.messagingrealtimegateway.service.MessageFanOutService;
import com.ziyadsamhaoui.messagingrealtimegateway.support.AbstractIntegrationTest;
import com.ziyadsamhaoui.messagingrealtimegateway.support.RedisFailureModeApp;
import com.ziyadsamhaoui.messagingrealtimegateway.support.StompTestClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

class MultiInstanceFanoutTest extends AbstractIntegrationTest {

    @Autowired
    MessageFanOutService fanOutOnInstanceA;

    @Autowired
    ObjectMapper objectMapper;

    private StompTestClient subscriberOn(int port, String userId) throws InterruptedException {
        StompTestClient client = new StompTestClient();
        String token = TOKENS.mint(userId);
        boolean connected = client.connect("ws://localhost:" + port + "/ws/websocket",
                h -> h.add("Authorization", "Bearer " + token), 10);
        assertThat(connected).as("subscriber on port " + port).isTrue();
        client.subscribe("/topic/rooms/42");
        Thread.sleep(250);
        return client;
    }

    private JsonNode payload(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Test
    void deduplicationIsSharedAcrossInstancesSoEachEventBroadcastsOnce() throws Exception {
        int portA = port();

        try (ConfigurableApplicationContext instanceB = new SpringApplicationBuilder(RedisFailureModeApp.class)
                .properties(
                        "server.port=0",
                        "spring.data.redis.host=" + REDIS.getHost(),
                        "spring.data.redis.port=" + REDIS.getMappedPort(6379),
                        "auth.jwks-uri=" + AUTH.url("/oauth2/jwks"),
                        "auth.issuer=http://localhost:8081",
                        "upstream.chat-service=" + CHAT.url("/"),
                        "upstream.user-service=" + USER.url("/"),
                        "upstream.user-service-internal-token=test-internal-token")
                .run()) {
            int portB = instanceB.getEnvironment().getProperty("local.server.port", Integer.class, 0);
            assertThat(portB).isPositive();
            MessageFanOutService fanOutOnInstanceB = instanceB.getBean(MessageFanOutService.class);

            StompTestClient subscriberA = subscriberOn(portA, "viewer-a");
            StompTestClient subscriberB = subscriberOn(portB, "viewer-b");

            fanOutOnInstanceA.fanOut("cluster-event-1", "MESSAGE_SENT",
                    payload("{\"messageId\":\"m1\",\"roomId\":\"42\",\"senderId\":\"s\",\"content\":\"from A\"}"));
            assertThat(subscriberA.nextFrame("/topic/rooms/42", 10)).isNotNull();
            assertThat(subscriberB.nextFrame("/topic/rooms/42", 1)).isNull();

            fanOutOnInstanceB.fanOut("cluster-event-1", "MESSAGE_SENT",
                    payload("{\"messageId\":\"m1\",\"roomId\":\"42\",\"senderId\":\"s\",\"content\":\"redelivered\"}"));
            assertThat(subscriberB.nextFrame("/topic/rooms/42", 1)).isNull();

            fanOutOnInstanceB.fanOut("cluster-event-2", "MESSAGE_SENT",
                    payload("{\"messageId\":\"m2\",\"roomId\":\"42\",\"senderId\":\"s\",\"content\":\"from B\"}"));
            assertThat(subscriberB.nextFrame("/topic/rooms/42", 10)).isNotNull();

            subscriberA.close();
            subscriberB.close();
        }
    }
}
