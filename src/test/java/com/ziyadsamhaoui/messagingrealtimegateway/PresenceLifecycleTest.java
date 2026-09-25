package com.ziyadsamhaoui.messagingrealtimegateway;

import com.ziyadsamhaoui.messagingrealtimegateway.service.PresenceService;
import com.ziyadsamhaoui.messagingrealtimegateway.support.AbstractIntegrationTest;
import com.ziyadsamhaoui.messagingrealtimegateway.support.StompTestClient;
import mockwebserver3.MockResponse;
import mockwebserver3.RecordedRequest;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class PresenceLifecycleTest extends AbstractIntegrationTest {

    @org.springframework.beans.factory.annotation.Autowired
    StringRedisTemplate redis;

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
    void connectWritesPresenceKeyWithTtl() {
        try (StompTestClient client = connectedClient("presence-user-1")) {
            String value = redis.opsForValue().get(PresenceService.key("presence-user-1"));
            assertThat(value).isEqualTo("ONLINE");
            Long ttl = redis.getExpire(PresenceService.key("presence-user-1"));
            assertThat(ttl).isNotNull();
            assertThat(ttl).isGreaterThan(0);
            assertThat(ttl).isLessThanOrEqualTo(30);
        }
    }

    @Test
    void cleanDisconnectDeletesPresenceAndPatchesLastSeen() throws Exception {
        try (StompTestClient client = connectedClient("presence-user-2")) {
            assertThat(Boolean.TRUE.equals(redis.hasKey(PresenceService.key("presence-user-2")))).isTrue();
            client.close();

            Thread.sleep(500);
            assertThat(Boolean.TRUE.equals(redis.hasKey(PresenceService.key("presence-user-2")))).isFalse();
            RecordedRequest patch = USER.takeRequest(5, TimeUnit.SECONDS);
            assertThat(patch).isNotNull();
            assertThat(patch.getMethod()).isEqualTo("PATCH");
            assertThat(patch.getTarget()).isEqualTo("/internal/users/presence-user-2/last-seen");
            assertThat(patch.getHeaders().get("X-Internal-Token")).isEqualTo("test-internal-token");
            assertThat(patch.getBody().utf8()).contains("seenAt");
        }
    }

    @Test
    void ttlExpiryWithLiveSessionFiresExactlyOneSweepRestore() throws Exception {

        try (StompTestClient client = connectedClient("presence-user-3")) {
            redis.delete(PresenceService.key("presence-user-3"));

            long deadline = System.currentTimeMillis() + 10_000;
            boolean restored = false;
            while (System.currentTimeMillis() < deadline) {
                if (Boolean.TRUE.equals(redis.hasKey(PresenceService.key("presence-user-3")))) {
                    restored = true;
                    break;
                }
                Thread.sleep(200);
            }
            assertThat(restored).as("sweeper restored the presence key").isTrue();

            Thread.sleep(6_000);
            assertThat(redis.opsForValue().get(PresenceService.key("presence-user-3"))).isEqualTo("ONLINE");
        }
    }

    @Test
    void lastSeenPatchIsRetriedOnceAndThenNeverBlocksCleanup() throws Exception {

        USER.enqueue(new MockResponse.Builder().code(500).build());
        USER.enqueue(new MockResponse.Builder().code(500).build());
        try (StompTestClient client = connectedClient("presence-user-4")) {
            client.close();
        }
        Thread.sleep(1_000);
        RecordedRequest first = USER.takeRequest(5, TimeUnit.SECONDS);
        RecordedRequest second = USER.takeRequest(2, TimeUnit.SECONDS);
        RecordedRequest third = USER.takeRequest(1, TimeUnit.SECONDS);
        assertThat(first).isNotNull();
        assertThat(second).isNotNull();
        assertThat(third).isNull();
    }
}
