package com.ziyadsamhaoui.messagingrealtimegateway.support;

import mockwebserver3.Dispatcher;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.QueueDispatcher;
import mockwebserver3.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(initializers = AbstractIntegrationTest.Initializer.class)
public abstract class AbstractIntegrationTest {

    public static final TestTokens TOKENS = new TestTokens();

    public static final MockWebServer AUTH = new MockWebServer();
    public static final MockWebServer CHAT = new MockWebServer();
    public static final MockWebServer USER = new MockWebServer();

    public static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

    private static final Map<String, Supplier<MockResponse>> CHAT_SCRIPTS = new ConcurrentHashMap<>();

    static {
        try {
            REDIS.start();
            AUTH.setDispatcher(new AuthDispatcher());
            AUTH.start();
            CHAT.setDispatcher(new ChatDispatcher());
            CHAT.start();
            USER.start();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    static class AuthDispatcher extends Dispatcher {
        @Override
        public MockResponse dispatch(RecordedRequest request) {
            return new MockResponse.Builder()
                    .code(200)
                    .body(TOKENS.jwksJson())
                    .addHeader("Content-Type", "application/json")
                    .build();
        }
    }

    static class ChatDispatcher extends QueueDispatcher {

        private final BlockingQueue<MockResponse> queued = new LinkedBlockingQueue<>();

        @Override
        public void enqueue(MockResponse response) {
            queued.add(response);
        }

        @Override
        public MockResponse dispatch(RecordedRequest request) {
            String key = request.getMethod() + " " + request.getTarget();
            Supplier<MockResponse> scripted = CHAT_SCRIPTS.get(key);
            if (scripted != null) {
                return scripted.get();
            }

            if ("GET".equals(request.getMethod()) && request.getTarget().startsWith("/rooms/")) {
                return chatRoomOk();
            }
            MockResponse enqueued = queued.poll();
            if (enqueued != null) {
                return enqueued;
            }

            if ("POST".equals(request.getMethod()) && request.getTarget().matches("/rooms/[^/]+/messages")) {
                return new MockResponse.Builder()
                        .code(201)
                        .body("{\"id\":\"msg-1\"}")
                        .addHeader("Content-Type", "application/json")
                        .build();
            }
            return new MockResponse.Builder().code(404).body("{\"status\":404,\"code\":\"ROOM_NOT_FOUND\",\"message\":\"no script\"}").build();
        }
    }

    public static void scriptChat(String methodAndPath, Supplier<MockResponse> response) {
        CHAT_SCRIPTS.put(methodAndPath, response);
    }

    public static void clearChatScripts() {
        CHAT_SCRIPTS.clear();
    }

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @Override
        public void initialize(ConfigurableApplicationContext context) {
            TestPropertyValues.of(
                    "spring.data.redis.host=" + REDIS.getHost(),
                    "spring.data.redis.port=" + REDIS.getMappedPort(6379),
                    "spring.data.redis.password=",
                    "auth.jwks-uri=" + AUTH.url("/oauth2/jwks"),
                    "auth.issuer=http://localhost:8081",
                    "upstream.chat-service=" + CHAT.url("/"),
                    "upstream.user-service=" + USER.url("/"),
                    "upstream.user-service-internal-token=test-internal-token",
                    "logging.level.com.ziyadsamhaoui.messagingrealtimegateway=DEBUG"
            ).applyTo(context.getEnvironment());
        }
    }

    @Autowired
    public org.springframework.core.env.Environment environment;

    public int port() {
        return environment.getProperty("local.server.port", Integer.class);
    }

    @BeforeEach
    void resetScripts() {
        clearChatScripts();
    }

    @AfterEach
    void cleanupScripts() {
        clearChatScripts();
    }

    public static MockResponse chatRoomOk() {
        return new MockResponse.Builder()
                .code(200)
                .body("{\"id\":\"42\"}")
                .addHeader("Content-Type", "application/json")
                .build();
    }

    public static MockResponse chatError(int status, String code, String message) {
        return new MockResponse.Builder()
                .code(status)
                .body("{\"timestamp\":\"2026-09-20T12:00:00Z\",\"status\":" + status
                        + ",\"code\":\"" + code + "\",\"message\":\"" + message + "\",\"path\":\"/rooms/42\"}")
                .addHeader("Content-Type", "application/json")
                .build();
    }

}
