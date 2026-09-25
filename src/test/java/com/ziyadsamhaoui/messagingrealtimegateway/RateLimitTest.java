package com.ziyadsamhaoui.messagingrealtimegateway;

import com.ziyadsamhaoui.messagingrealtimegateway.support.AbstractIntegrationTest;
import com.ziyadsamhaoui.messagingrealtimegateway.support.StompTestClient;
import mockwebserver3.MockResponse;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RateLimitTest extends AbstractIntegrationTest {

    private String wsUrl() {
        return "ws://localhost:" + port() + "/ws/websocket";
    }

    private StompTestClient connectedClient(String userId) {
        StompTestClient client = new StompTestClient();
        String token = TOKENS.mint(userId);
        boolean ok = client.connect(wsUrl(), h -> h.add("Authorization", "Bearer " + token), 5);
        if (!ok) {
            client.close();
            throw new IllegalStateException("client failed to connect: " + client.connectError());
        }
        return client;
    }

    @Test
    void burstPastBudgetIsRejectedAndChatIsNeverCalledForTheExcess() throws Exception {
        int burst = 20;

        for (int i = 0; i < burst; i++) {
            CHAT.enqueue(new MockResponse.Builder().code(201).build());
        }

        try (StompTestClient client = connectedClient("burst-user")) {
            client.subscribe("/user/queue/errors");
            for (int i = 0; i < burst + 5; i++) {
                client.send("/app/chat.sendMessage",
                        "{\"roomId\":\"42\",\"type\":\"TEXT\",\"content\":\"msg " + i + "\"}");
            }

            boolean sawRateLimited = false;
            for (int i = 0; i < 5; i++) {
                String error = client.nextFrame("/user/queue/errors", 5);
                if (error != null && error.contains("RATE_LIMITED")) {
                    sawRateLimited = true;
                }
            }
            assertThat(sawRateLimited).isTrue();

            int forwarded = 0;
            while (CHAT.takeRequest(500, java.util.concurrent.TimeUnit.MILLISECONDS) != null) {
                forwarded++;
            }
            assertThat(forwarded).isEqualTo(burst);
        }
    }

    @Test
    void messageAndTypingBucketsAreIndependent() throws Exception {

        try (StompTestClient client = connectedClient("typing-user")) {
            for (int i = 0; i < 15; i++) {
                client.send("/app/chat.typing", "{\"roomId\":\"42\"}");
            }
        }

        CHAT.enqueue(new MockResponse.Builder().code(201).build());
        try (StompTestClient client = connectedClient("typing-user")) {
            client.send("/app/chat.sendMessage",
                    "{\"roomId\":\"42\",\"type\":\"TEXT\",\"content\":\"still allowed\"}");
            var recorded = CHAT.takeRequest(5, java.util.concurrent.TimeUnit.SECONDS);
            assertThat(recorded).isNotNull();
            assertThat(recorded.getTarget()).isEqualTo("/rooms/42/messages");
        }
    }

    @Test
    void overBudgetTypingIsDroppedSilentlyWithoutErrorFrame() throws Exception {
        try (StompTestClient client = connectedClient("typing-flood")) {
            client.subscribe("/user/queue/errors");
            for (int i = 0; i < 12; i++) {
                client.send("/app/chat.typing", "{\"roomId\":\"42\"}");
            }

            assertThat(client.nextFrame("/user/queue/errors", 2)).isNull();
        }
    }

    @Test
    void budgetIsSharedAcrossReconnectsBecauseBucketsAreRedisBacked() throws Exception {

        for (int i = 0; i < 18; i++) {
            CHAT.enqueue(new MockResponse.Builder().code(201).build());
        }
        try (StompTestClient client = connectedClient("shared-budget")) {
            for (int i = 0; i < 18; i++) {
                client.send("/app/chat.sendMessage",
                        "{\"roomId\":\"42\",\"type\":\"TEXT\",\"content\":\"m" + i + "\"}");
            }
        }

        CHAT.enqueue(new MockResponse.Builder().code(201).build());
        try (StompTestClient client = connectedClient("shared-budget")) {
            client.send("/app/chat.sendMessage",
                    "{\"roomId\":\"42\",\"type\":\"TEXT\",\"content\":\"one more\"}");
            var recorded = CHAT.takeRequest(5, java.util.concurrent.TimeUnit.SECONDS);
            assertThat(recorded).isNotNull();
        }

        try (StompTestClient client = connectedClient("shared-budget")) {
            client.subscribe("/user/queue/errors");
            client.send("/app/chat.sendMessage", "{\"roomId\":\"42\",\"type\":\"TEXT\",\"content\":\"x\"}");
            client.send("/app/chat.sendMessage", "{\"roomId\":\"42\",\"type\":\"TEXT\",\"content\":\"y\"}");
            client.send("/app/chat.sendMessage", "{\"roomId\":\"42\",\"type\":\"TEXT\",\"content\":\"z\"}");
            boolean rejected = false;
            for (int i = 0; i < 3; i++) {
                String error = client.nextFrame("/user/queue/errors", 5);
                if (error != null && error.contains("RATE_LIMITED")) {
                    rejected = true;
                }
            }
            assertThat(rejected).isTrue();
        }
    }
}
