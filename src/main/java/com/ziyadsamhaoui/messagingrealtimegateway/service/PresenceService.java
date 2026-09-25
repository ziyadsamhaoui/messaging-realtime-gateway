package com.ziyadsamhaoui.messagingrealtimegateway.service;

import com.ziyadsamhaoui.messagingrealtimegateway.client.UserServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;

@Service
public class PresenceService {

    static final String PRESENCE_PREFIX = "presence:";

    private static final Logger log = LoggerFactory.getLogger(PresenceService.class);

    private final StringRedisTemplate redis;
    private final UserServiceClient userServiceClient;
    private final Duration ttl;
    private final java.util.concurrent.ConcurrentHashMap<String, Integer> localRefCounts =
            new java.util.concurrent.ConcurrentHashMap<>();

    public PresenceService(StringRedisTemplate redis,
                           UserServiceClient userServiceClient,
                           @Value("${realtime.presence-ttl-seconds}") long ttlSeconds) {
        this.redis = redis;
        this.userServiceClient = userServiceClient;
        this.ttl = Duration.ofSeconds(ttlSeconds);
    }

    public void onConnect(String userId) {
        localRefCounts.merge(userId, 1, Integer::sum);
        setPresence(userId);
    }

    public void onHeartbeat(String userId) {
        setPresence(userId);
    }

    public void onDisconnect(String userId) {
        int remaining = localRefCounts.merge(userId, -1, Integer::sum);
        if (remaining > 0) {
            return;
        }
        localRefCounts.remove(userId);
        deletePresence(userId);
        userServiceClient.updateLastSeen(userId, Instant.now());
    }

    public void refreshForSweptUser(String userId) {
        Integer refs = localRefCounts.get(userId);
        if (refs != null && refs > 0) {
            setPresence(userId);
        }
    }

    private void setPresence(String userId) {
        try {
            redis.opsForValue().set(key(userId), "ONLINE", ttl);
        } catch (Exception e) {
            log.debug("Presence write failed for {} (failing open): {}", userId, e.getMessage());
        }
    }

    private void deletePresence(String userId) {
        try {
            redis.delete(key(userId));
        } catch (Exception e) {
            log.debug("Presence delete failed for {} (failing open): {}", userId, e.getMessage());
        }
    }

    public static String key(String userId) {
        return PRESENCE_PREFIX + userId;
    }
}
