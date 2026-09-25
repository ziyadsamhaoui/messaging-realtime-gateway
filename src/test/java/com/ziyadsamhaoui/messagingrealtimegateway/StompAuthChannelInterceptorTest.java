package com.ziyadsamhaoui.messagingrealtimegateway;

import com.ziyadsamhaoui.messagingrealtimegateway.service.PresenceService;
import com.ziyadsamhaoui.messagingrealtimegateway.support.AbstractIntegrationTest;
import com.ziyadsamhaoui.messagingrealtimegateway.support.StompTestClient;
import com.ziyadsamhaoui.messagingrealtimegateway.support.TestTokens;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;

import static org.assertj.core.api.Assertions.assertThat;

class StompAuthChannelInterceptorTest extends AbstractIntegrationTest {

    @Autowired
    StringRedisTemplate redis;

    private String wsUrl() {
        return "ws://localhost:" + port() + "/ws/websocket";
    }

    private boolean presenceKeyExists(String userId) {
        Boolean exists = redis.hasKey(PresenceService.key(userId));
        return Boolean.TRUE.equals(exists);
    }

    @Test
    void connectWithoutAuthorizationIsRejected() throws Exception {
        try (StompTestClient client = new StompTestClient()) {
            boolean connected = client.connect(wsUrl(), h -> {}, 5);
            assertThat(connected).isFalse();
            assertThat(client.connectError()).isNotNull();
        }
    }

    @Test
    void connectWithExpiredTokenIsRejected() throws Exception {
        String expired = TOKENS.mintExpired("user-expired");
        try (StompTestClient client = new StompTestClient()) {
            boolean connected = client.connect(wsUrl(),
                    h -> h.add("Authorization", "Bearer " + expired), 5);
            assertThat(connected).isFalse();
            assertThat(client.connectError()).isNotNull();
            assertThat(presenceKeyExists("user-expired")).isFalse();
        }
    }

    @Test
    void connectWithGarbageTokenIsRejected() {
        try (StompTestClient client = new StompTestClient()) {
            boolean connected = client.connect(wsUrl(),
                    h -> h.add("Authorization", "Bearer not.a.jwt"), 5);
            assertThat(connected).isFalse();
        }
    }

    @Test
    void connectWithWrongKeyTokenIsRejected() {
        TestTokens otherRealm = new TestTokens();
        String foreign = otherRealm.mint("user-foreign");
        try (StompTestClient client = new StompTestClient()) {
            boolean connected = client.connect(wsUrl(),
                    h -> h.add("Authorization", "Bearer " + foreign), 5);
            assertThat(connected).isFalse();
        }
    }

    @Test
    void validConnectSetsPrincipalAndPresence() throws Exception {
        String token = TOKENS.mint("user-alice");
        try (StompTestClient client = new StompTestClient()) {
            boolean connected = client.connect(wsUrl(),
                    h -> h.add("Authorization", "Bearer " + token), 5);
            assertThat(connected).as(() -> "connect error: " + client.connectError()).isTrue();
            assertThat(presenceKeyExists("user-alice")).isTrue();

        }
    }
}
