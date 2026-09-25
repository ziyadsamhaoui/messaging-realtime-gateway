package com.ziyadsamhaoui.messagingrealtimegateway;

import com.ziyadsamhaoui.messagingrealtimegateway.support.AbstractIntegrationTest;
import com.ziyadsamhaoui.messagingrealtimegateway.support.StompTestClient;
import mockwebserver3.MockResponse;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StompSubscriptionAuthorizationTest extends AbstractIntegrationTest {

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
    void memberSubscriptionIsAllowed() {

        try (StompTestClient client = connectedClient("member-1")) {
            client.subscribeAndDiscard("/topic/rooms/42");

        }
    }

    @Test
    void nonMemberSubscriptionIsRefusedWithChatsCodeRelayedVerbatim() throws Exception {
        scriptChat("GET /rooms/42",
                () -> chatError(403, "ROOM_ACCESS_DENIED", "The caller is not a participant of this room"));
        try (StompTestClient client = connectedClient("outsider-1")) {
            client.subscribeAndDiscard("/topic/rooms/42");
            assertThat(client.awaitError(5)).isTrue();
            assertThat(client.connectError()).contains("ROOM_ACCESS_DENIED");
            assertThat(client.connectError()).contains("The caller is not a participant of this room");
        }
    }

    @Test
    void unknownRoomIsRefusedWithRoomNotFound() throws Exception {
        scriptChat("GET /rooms/999",
                () -> chatError(404, "ROOM_NOT_FOUND", "Room not found"));
        try (StompTestClient client = connectedClient("member-2")) {
            client.subscribeAndDiscard("/topic/rooms/999");
            assertThat(client.awaitError(5)).isTrue();
            assertThat(client.connectError()).contains("ROOM_NOT_FOUND");
        }
    }

    @Test
    void typingTopicIsGatedSameAsMessagesTopic() throws Exception {
        scriptChat("GET /rooms/42",
                () -> chatError(403, "ROOM_ACCESS_DENIED", "The caller is not a participant of this room"));
        try (StompTestClient client = connectedClient("outsider-2")) {
            client.subscribeAndDiscard("/topic/rooms/42/typing");
            assertThat(client.awaitError(5)).isTrue();
            assertThat(client.connectError()).contains("ROOM_ACCESS_DENIED");
        }
    }

    @Test
    void chatUnreachableRefusesSubscriptionFailClosed() throws Exception {
        scriptChat("GET /rooms/42",
                () -> new MockResponse.Builder()
                        .code(503)
                        .body("{\"status\":503,\"code\":\"SERVICE_UNAVAILABLE\",\"message\":\"Chat is overloaded\"}")
                        .addHeader("Content-Type", "application/json")
                        .build());
        try (StompTestClient client = connectedClient("member-3")) {
            client.subscribeAndDiscard("/topic/rooms/42");
            assertThat(client.awaitError(5)).isTrue();

            assertThat(client.connectError()).contains("SERVICE_UNAVAILABLE");
        }
    }

    @Test
    void refusalHappensBeforeAnyDeliveryIsRegistered() throws Exception {
        scriptChat("GET /rooms/42",
                () -> chatError(403, "ROOM_ACCESS_DENIED", "denied"));
        try (StompTestClient client = connectedClient("outsider-3")) {
            client.subscribe("/topic/rooms/42");
            assertThat(client.awaitError(5)).isTrue();

            assertThat(client.nextFrame("/topic/rooms/42", 1)).isNull();
        }
    }
}
