package com.ziyadsamhaoui.messagingrealtimegateway;

import com.ziyadsamhaoui.messagingrealtimegateway.support.AbstractIntegrationTest;
import com.ziyadsamhaoui.messagingrealtimegateway.support.RedisFailureModeApp;
import com.ziyadsamhaoui.messagingrealtimegateway.support.StompTestClient;
import mockwebserver3.MockResponse;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

class MultiInstanceFanoutTest extends AbstractIntegrationTest {

    @Test
    void sendOnInstanceADeliversToSubscriberOnInstanceB() throws Exception {

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
            String portBProperty = instanceB.getEnvironment().getProperty("local.server.port");
            int portB = portBProperty != null ? Integer.parseInt(portBProperty) : 0;
            assertThat(portB).isPositive();

            CHAT.enqueue(chatRoomOk());
            StompTestClient recipient = new StompTestClient();
            String recipientToken = TOKENS.mint("recipient-on-b");
            boolean connected = recipient.connect("ws://localhost:" + portB + "/ws/websocket",
                    h -> h.add("Authorization", "Bearer " + recipientToken), 10);
            assertThat(connected).as("recipient connected to instance B").isTrue();
            recipient.subscribe("/topic/rooms/42");

            CHAT.enqueue(new MockResponse.Builder().code(201).build());
            StompTestClient sender = new StompTestClient();
            String senderToken = TOKENS.mint("sender-on-a");
            boolean senderConnected = sender.connect("ws://localhost:" + portA + "/ws/websocket",
                    h -> h.add("Authorization", "Bearer " + senderToken), 10);
            assertThat(senderConnected).as("sender connected to instance A").isTrue();

            sender.send("/app/chat.sendMessage",
                    "{\"roomId\":\"42\",\"type\":\"TEXT\",\"content\":\"cross-instance hello\"}");

            String received = recipient.nextFrame("/topic/rooms/42", 10);
            assertThat(received).isNotNull();
            assertThat(received).contains("cross-instance hello");

            sender.close();
            recipient.close();
        }
    }
}
