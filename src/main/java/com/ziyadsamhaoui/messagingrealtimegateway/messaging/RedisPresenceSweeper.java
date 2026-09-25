package com.ziyadsamhaoui.messagingrealtimegateway.messaging;

import com.ziyadsamhaoui.messagingrealtimegateway.service.PresenceService;
import com.ziyadsamhaoui.messagingrealtimegateway.stomp.StompSessionRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.security.Principal;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class RedisPresenceSweeper {

    private static final Logger log = LoggerFactory.getLogger(RedisPresenceSweeper.class);
    private static final long PERIOD_MS = 5000;

    private final PresenceService presenceService;
    private final StompSessionRegistry registry;
    private final StringRedisTemplate redis;
    private final ScheduledExecutorService scheduler;

    public RedisPresenceSweeper(PresenceService presenceService,
                                StompSessionRegistry registry,
                                StringRedisTemplate redis,
                                ScheduledExecutorService presenceSweeperExecutor) {
        this.presenceService = presenceService;
        this.registry = registry;
        this.redis = redis;
        this.scheduler = presenceSweeperExecutor;
    }

    @jakarta.annotation.PostConstruct
    public void start() {
        scheduler.scheduleWithFixedDelay(this::sweep, PERIOD_MS, PERIOD_MS, TimeUnit.MILLISECONDS);
    }

    void sweep() {
        for (String sessionId : registry.sessionIds()) {
            Principal user = registry.getUser(sessionId);
            if (user == null) {
                continue;
            }
            String userId = user.getName();
            try {
                Boolean exists = redis.hasKey(PresenceService.key(userId));
                if (!Boolean.TRUE.equals(exists)) {
                    log.debug("Presence key missing for live local session of user {}; restoring", userId);
                    presenceService.refreshForSweptUser(userId);
                }
            } catch (Exception e) {
                log.debug("Presence sweep skipped (Redis unavailable): {}", e.getMessage());
            }
        }
    }
}
