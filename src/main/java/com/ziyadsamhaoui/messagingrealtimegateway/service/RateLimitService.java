package com.ziyadsamhaoui.messagingrealtimegateway.service;

import com.ziyadsamhaoui.messagingrealtimegateway.exception.RateLimitCheckUnavailableException;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.ConfigurationBuilder;
import io.github.bucket4j.distributed.proxy.ProxyManager;
import io.github.bucket4j.redis.lettuce.cas.LettuceBasedProxyManager;
import io.lettuce.core.RedisClient;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

@Service
public class RateLimitService {

    public static final String MESSAGE_BUCKET_PREFIX = "msgrate:";
    public static final String TYPING_BUCKET_PREFIX = "typingrate:";

    private static final Logger log = LoggerFactory.getLogger(RateLimitService.class);

    private final RedisClient lettuceClient;
    private volatile ProxyManager<byte[]> proxyManager;
    private final BucketConfiguration messageConfig;
    private final BucketConfiguration typingConfig;
    private final Counter messageRejections;
    private final Counter typingRejections;

    public RateLimitService(
            @Value("${spring.data.redis.host}") String host,
            @Value("${spring.data.redis.port}") int port,
            @Value("${spring.data.redis.password:}") String password,
            @Value("${realtime.message-rate-replenish}") long messageReplenish,
            @Value("${realtime.message-rate-burst}") long messageBurst,
            @Value("${realtime.typing-rate-replenish}") long typingReplenish,
            @Value("${realtime.typing-rate-burst}") long typingBurst,
            MeterRegistry meterRegistry) {

        this.lettuceClient = (password != null && !password.isBlank())
                ? RedisClient.create("redis://:" + password + "@" + host + ":" + port)
                : RedisClient.create("redis://" + host + ":" + port);

        this.messageConfig = bucketConfig(messageReplenish, messageBurst);
        this.typingConfig = bucketConfig(typingReplenish, typingBurst);

        // Two separate counters: a spike on the message limit is a very different incident
        // from typing being throttled, and one merged metric would hide which limit is hit.
        this.messageRejections = rejectionCounter(meterRegistry, "message");
        this.typingRejections = rejectionCounter(meterRegistry, "typing");
    }

    private static Counter rejectionCounter(MeterRegistry meterRegistry, String bucket) {
        return Counter.builder("badrlink.realtime.ratelimit.rejections")
                .description("Rejected WebSocket send attempts, per rate-limit bucket")
                .tag("bucket", bucket)
                .register(meterRegistry);
    }

    @jakarta.annotation.PreDestroy
    void shutdown() {
        lettuceClient.shutdown();
    }

    private static BucketConfiguration bucketConfig(long replenish, long burst) {
        ConfigurationBuilder builder = BucketConfiguration.builder();
        builder.addLimit(Bandwidth.builder()
                .capacity(burst)
                .refillGreedy(replenish, Duration.ofSeconds(1))
                .build());
        return builder.build();
    }

    public enum Outcome {

        ALLOWED,

        REJECTED,

        UNAVAILABLE
    }

    public Outcome checkMessageSend(String userId) {
        return recordRejection(tryConsume(MESSAGE_BUCKET_PREFIX + userId, messageConfig, true),
                messageRejections);
    }

    public Outcome checkTyping(String userId) {
        return recordRejection(tryConsume(TYPING_BUCKET_PREFIX + userId, typingConfig, false),
                typingRejections);
    }

    private static Outcome recordRejection(Outcome outcome, Counter counter) {
        if (outcome == Outcome.REJECTED) {
            counter.increment();
        }
        return outcome;
    }

    // LettuceBasedProxyManager.builderFor() opens a connection immediately, so building it in the
    // constructor makes a Redis outage a startup failure. Build it on first use instead: the
    // gateway keeps running and message sends fail closed until Redis is reachable again.
    private ProxyManager<byte[]> proxyManager() {
        ProxyManager<byte[]> manager = proxyManager;
        if (manager == null) {
            synchronized (this) {
                manager = proxyManager;
                if (manager == null) {
                    manager = LettuceBasedProxyManager.builderFor(lettuceClient).build();
                    proxyManager = manager;
                }
            }
        }
        return manager;
    }

    private Outcome tryConsume(String key, BucketConfiguration config, boolean failClosed) {
        try {
            io.github.bucket4j.distributed.BucketProxy bucket =
                    proxyManager().builder().build(key.getBytes(StandardCharsets.UTF_8), config);
            return bucket.tryConsume(1) ? Outcome.ALLOWED : Outcome.REJECTED;
        } catch (Exception e) {
            log.debug("Rate-limit check unavailable for key {} (failing {}): {}",
                    key, failClosed ? "closed" : "open", e.getMessage());
            if (failClosed) {
                throw new RateLimitCheckUnavailableException("Rate limiter unavailable", e);
            }
            return Outcome.ALLOWED;
        }
    }
}
