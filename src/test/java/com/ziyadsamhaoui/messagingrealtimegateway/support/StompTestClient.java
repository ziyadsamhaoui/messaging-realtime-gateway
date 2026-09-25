package com.ziyadsamhaoui.messagingrealtimegateway.support;

import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompFrameHandler;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class StompTestClient implements AutoCloseable {

    private final WebSocketStompClient stompClient;
    private final AtomicReference<StompSession> session = new AtomicReference<>();
    private final AtomicReference<String> connectError = new AtomicReference<>();
    private final Map<String, BlockingQueue<String>> frames = new java.util.concurrent.ConcurrentHashMap<>();

    public StompTestClient() {
        this.stompClient = new WebSocketStompClient(new StandardWebSocketClient());
        this.stompClient.setDefaultHeartbeat(new long[]{0, 0});
        this.stompClient.setTaskScheduler(new org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler() {{
            setPoolSize(1);
            setThreadNamePrefix("stomp-test-client");
            setDaemon(true);
            initialize();
        }});
    }

    public boolean connect(String url, java.util.function.Consumer<StompHeaders> headersCustomizer,
                           long timeoutSeconds) {
        StompHeaders connectHeaders = new StompHeaders();
        if (headersCustomizer != null) {
            headersCustomizer.accept(connectHeaders);
        }
        StompSessionHandlerAdapter handler = new StompSessionHandlerAdapter() {
            @Override
            public void afterConnected(StompSession s, StompHeaders connectedHeaders) {
                session.set(s);
            }

            @Override
            public void handleFrame(StompHeaders headers, Object payload) {

                connectError.compareAndSet(null, describePayload(payload));
            }

            @Override
            public void handleException(StompSession s, StompCommand command, StompHeaders headers,
                                        byte[] payload, Throwable exception) {
                connectError.compareAndSet(null, describePayload(payload) + " / " + exception);
            }

            @Override
            public void handleTransportError(StompSession s, Throwable exception) {
                connectError.compareAndSet(null, "transport: " + exception.getMessage());
            }
        };
        try {
            org.springframework.web.socket.WebSocketHttpHeaders wsHeaders =
                    new org.springframework.web.socket.WebSocketHttpHeaders();
            wsHeaders.add("Origin", "http://localhost");
            java.util.concurrent.CompletableFuture<StompSession> future =
                    stompClient.connectAsync(url, wsHeaders, connectHeaders, handler);
            try {
                session.set(future.get(timeoutSeconds, TimeUnit.SECONDS));
            } catch (java.util.concurrent.TimeoutException te) {

                connectError.compareAndSet(null, "timeout waiting for CONNECTED (future incomplete)");
                future.cancel(true);
            }
            return session.get() != null && session.get().isConnected();
        } catch (Exception e) {
            if (connectError.get() == null) {
                connectError.set("connect failed: " + e.getMessage());
            }
            return false;
        }
    }

    public String connectError() {
        return connectError.get();
    }

    private static String describePayload(Object payload) {
        if (payload instanceof byte[] b) {
            return new String(b, java.nio.charset.StandardCharsets.UTF_8);
        }
        return String.valueOf(payload);
    }

    public StompSession session() {
        return session.get();
    }

    public void subscribe(String destination) {
        session.get().subscribe(destination, new StompFrameHandler() {
            @Override
            public Type getPayloadType(StompHeaders headers) {
                return byte[].class;
            }

            @Override
            public void handleFrame(StompHeaders headers, Object payload) {
                frames.computeIfAbsent(destination, d -> new LinkedBlockingQueue<>())
                        .add(payload instanceof byte[] b ? new String(b) : String.valueOf(payload));
            }
        });
    }

    public void subscribeAndDiscard(String destination) {
        session.get().subscribe(destination, new StompFrameHandler() {
            @Override
            public Type getPayloadType(StompHeaders headers) {
                return byte[].class;
            }

            @Override
            public void handleFrame(StompHeaders headers, Object payload) {
            }
        });
    }

    public void send(String destination, String jsonPayload) {

        session.get().send(destination, jsonPayload.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    public String nextFrame(String destination, long timeoutSeconds) throws InterruptedException {
        BlockingQueue<String> queue = frames.get(destination);
        return queue == null ? null : queue.poll(timeoutSeconds, TimeUnit.SECONDS);
    }

    public boolean awaitError(long timeoutSeconds) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000;
        while (connectError.get() == null && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        return connectError.get() != null;
    }

    @Override
    public void close() {
        StompSession s = session.get();
        if (s != null && s.isConnected()) {
            try {
                s.disconnect();
            } catch (Exception ignored) {
            }
        }
    }
}
