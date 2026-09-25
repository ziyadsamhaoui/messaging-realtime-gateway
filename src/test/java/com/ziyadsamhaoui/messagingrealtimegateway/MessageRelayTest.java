package com.ziyadsamhaoui.messagingrealtimegateway;

import com.ziyadsamhaoui.messagingrealtimegateway.support.AbstractIntegrationTest;
import com.ziyadsamhaoui.messagingrealtimegateway.support.StompTestClient;
import mockwebserver3.MockResponse;
import mockwebserver3.RecordedRequest;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class MessageRelayTest extends AbstractIntegrationTest {

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
    void sendForwardsCallersOwnAuthorizationToChatAndPublishes() throws Exception {
        try (StompTestClient client = connectedClient("sender-1")) {
            client.subscribe("/topic/rooms/42");
            client.send("/app/chat.sendMessage",
                    "{\"roomId\":\"42\",\"type\":\"TEXT\",\"content\":\"hello\"}");

            RecordedRequest post = null;
            for (int i = 0; i < 10 && post == null; i++) {
                RecordedRequest candidate = CHAT.takeRequest(2, TimeUnit.SECONDS);
                if (candidate != null && "POST".equals(candidate.getMethod())) {
                    post = candidate;
                }
            }
            assertThat(post).isNotNull();
            assertThat(post.getTarget()).isEqualTo("/rooms/42/messages");

            assertThat(post.getHeaders().get("Authorization")).startsWith("Bearer ");
            assertThat(post.getBody().utf8()).contains("\"content\":\"hello\"");

            assertThat(client.nextFrame("/topic/rooms/42", 5)).isNotNull();
        }
    }

    @Test
    void chatRejectionIsRelayedVerbatimToSenderOnly() throws Exception {
        scriptChat("POST /rooms/42/messages",
                () -> chatError(403, "PARTICIPANT_MUTED", "You are muted in this room"));
        try (StompTestClient sender = connectedClient("muted-sender")) {
            sender.subscribe("/topic/rooms/42");
            sender.subscribe("/user/queue/errors");
            sender.send("/app/chat.sendMessage",
                    "{\"roomId\":\"42\",\"type\":\"TEXT\",\"content\":\"hello?\"}");

            String error = sender.nextFrame("/user/queue/errors", 5);
            assertThat(error).isNotNull();
            assertThat(error).contains("PARTICIPANT_MUTED");
            assertThat(error).contains("You are muted in this room");
            assertThat(error).contains("/app/chat.sendMessage");

            assertThat(sender.nextFrame("/topic/rooms/42", 1)).isNull();
        }
    }

    @Test
    void chatUnavailableSurfacesUpstreamUnavailable() throws Exception {
        scriptChat("POST /rooms/42/messages",
                () -> chatError(500, "INTERNAL_SERVER_ERROR", "boom"));
        try (StompTestClient client = connectedClient("sender-2")) {
            client.subscribe("/user/queue/errors");
            client.send("/app/chat.sendMessage",
                    "{\"roomId\":\"42\",\"type\":\"TEXT\",\"content\":\"hello\"}");
            String error = client.nextFrame("/user/queue/errors", 5);
            assertThat(error).isNotNull();
            assertThat(error).contains("UPSTREAM_UNAVAILABLE");
        }
    }

    @Test
    void noIdentityHeaderIsEverMintedForChatCalls() throws Exception {
        try (StompTestClient client = connectedClient("sender-3")) {
            client.send("/app/chat.sendMessage",
                    "{\"roomId\":\"42\",\"type\":\"TEXT\",\"content\":\"hello\"}");
            RecordedRequest post = null;
            for (int i = 0; i < 10 && post == null; i++) {
                RecordedRequest candidate = CHAT.takeRequest(2, TimeUnit.SECONDS);
                if (candidate != null && "POST".equals(candidate.getMethod())) {
                    post = candidate;
                }
            }
            assertThat(post).isNotNull();
            assertThat(post.getHeaders().get("X-Internal-Token")).isNull();
        }
    }
}
