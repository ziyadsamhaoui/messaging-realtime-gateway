package com.ziyadsamhaoui.messagingrealtimegateway.service;

import com.ziyadsamhaoui.messagingrealtimegateway.exception.RateLimitCheckUnavailableException;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.ConfigurationBuilder;
import io.github.bucket4j.distributed.proxy.ProxyManager;
import io.github.bucket4j.redis.lettuce.cas.LettuceBasedProxyManager;
import io.lettuce.core.RedisClient;
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
    private final ProxyManager<byte[]> proxyManager;
    private final BucketConfiguration messageConfig;
    private final BucketConfiguration typingConfig;

    public RateLimitService(
            @Value("${spring.data.redis.host}") String host,
            @Value("${spring.data.redis.port}") int port,
            @Value("${spring.data.redis.password:}") String password,
            @Value("${realtime.message-rate-replenish}") long messageReplenish,
            @Value("${realtime.message-rate-burst}") long messageBurst,
            @Value("${realtime.typing-rate-replenish}") long typingReplenish,
            @Value("${realtime.typing-rate-burst}") long typingBurst) {

        this.lettuceClient = (password != null && !password.isBlank())
                ? RedisClient.create("redis://:" + password + "@" + host + ":" + port)
                : RedisClient.create("redis://" + host + ":" + port);
        this.proxyManager = LettuceBasedProxyManager.builderFor(lettuceClient).build();

        this.messageConfig = bucketConfig(messageReplenish, messageBurst);
        this.typingConfig = bucketConfig(typingReplenish, typingBurst);
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
        return tryConsume(MESSAGE_BUCKET_PREFIX + userId, messageConfig, true);
    }

    public Outcome checkTyping(String userId) {
        return tryConsume(TYPING_BUCKET_PREFIX + userId, typingConfig, false);
    }

    private Outcome tryConsume(String key, BucketConfiguration config, boolean failClosed) {
        try {
            io.github.bucket4j.distributed.BucketProxy bucket =
                    proxyManager.builder().build(key.getBytes(StandardCharsets.UTF_8), config);
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
