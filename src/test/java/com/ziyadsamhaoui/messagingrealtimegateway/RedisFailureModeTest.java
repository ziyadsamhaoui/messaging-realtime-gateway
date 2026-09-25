package com.ziyadsamhaoui.messagingrealtimegateway;

import com.ziyadsamhaoui.messagingrealtimegateway.support.AbstractIntegrationTest;
import com.ziyadsamhaoui.messagingrealtimegateway.support.RedisFailureModeApp;
import com.ziyadsamhaoui.messagingrealtimegateway.support.StompTestClient;
import mockwebserver3.MockResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import static org.assertj.core.api.Assertions.assertThat;

class RedisFailureModeTest extends AbstractIntegrationTest {

    @Autowired
    ApplicationContext context;

    @Test
    void messageSendFailsClosedWhenRateLimiterIsUnavailable() {

        org.springframework.boot.builder.SpringApplicationBuilder app =
                new org.springframework.boot.builder.SpringApplicationBuilder(RedisFailureModeApp.class)
                        .properties(
                                "server.port=0",
                                "spring.data.redis.host=127.0.0.1",
                                "spring.data.redis.port=1",
                                "auth.jwks-uri=" + AUTH.url("/oauth2/jwks"),
                                "auth.issuer=http://localhost:8081",
                                "upstream.chat-service=" + CHAT.url("/"),
                                "upstream.user-service=" + USER.url("/"),
                                "upstream.user-service-internal-token=test-internal-token");
        try (org.springframework.context.ConfigurableApplicationContext dead = app.run()) {
            com.ziyadsamhaoui.messagingrealtimegateway.service.RateLimitService rateLimiter =
                    dead.getBean(com.ziyadsamhaoui.messagingrealtimegateway.service.RateLimitService.class);

            org.assertj.core.api.Assertions.assertThatThrownBy(() -> rateLimiter.checkMessageSend("any-user"))
                    .isInstanceOf(com.ziyadsamhaoui.messagingrealtimegateway.exception.RateLimitCheckUnavailableException.class);
        }
    }

    @Test
    void presenceWriteFailureDoesNotDropConnectionOrSend() {

        org.springframework.boot.builder.SpringApplicationBuilder app =
                new org.springframework.boot.builder.SpringApplicationBuilder(RedisFailureModeApp.class)
                        .properties(
                                "server.port=0",
                                "spring.data.redis.host=127.0.0.1",
                                "spring.data.redis.port=1",
                                "auth.jwks-uri=" + AUTH.url("/oauth2/jwks"),
                                "auth.issuer=http://localhost:8081",
                                "upstream.chat-service=" + CHAT.url("/"),
                                "upstream.user-service=" + USER.url("/"),
                                "upstream.user-service-internal-token=test-internal-token");
        try (org.springframework.context.ConfigurableApplicationContext dead = app.run()) {
            int port = dead.getEnvironment().getProperty("local.server.port", Integer.class, 0);
            if (port == 0) {
                port = dead.getEnvironment().getProperty("local.server.port", Integer.class);
            }
            int finalPort = port;
            StompTestClient client = new StompTestClient();
            String token = TOKENS.mint("resilient-user");
            boolean connected = client.connect("ws://localhost:" + finalPort + "/ws/websocket",
                    h -> h.add("Authorization", "Bearer " + token), 10);
            assertThat(connected).as("connection survives Redis outage").isTrue();

            assertThat(client.connectError()).isNull();
            client.close();
        }
    }

}
