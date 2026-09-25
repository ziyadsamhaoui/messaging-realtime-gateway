package com.ziyadsamhaoui.messagingrealtimegateway.client;

import com.ziyadsamhaoui.messagingrealtimegateway.dto.Dtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class UserServiceClient {

    private static final Logger log = LoggerFactory.getLogger(UserServiceClient.class);

    private final RestClient restClient;
    private final ExecutorService executor;

    public UserServiceClient(@Qualifier("userServiceRestClient") RestClient restClient) {
        this.restClient = restClient;
        this.executor = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "user-last-seen");
            t.setDaemon(true);
            return t;
        });
    }

    public void updateLastSeen(String userId, Instant seenAt) {
        executor.submit(() -> updateLastSeenBlocking(userId, seenAt));
    }

    void updateLastSeenBlocking(String userId, Instant seenAt) {
        attempt(userId, seenAt, true);
    }

    private void attempt(String userId, Instant seenAt, boolean mayRetry) {
        try {
            restClient.patch()
                    .uri("/internal/users/{id}/last-seen", userId)
                    .body(new Dtos.LastSeenRequest(seenAt))
                    .retrieve()
                    .toBodilessEntity();
        } catch (org.springframework.web.client.HttpClientErrorException e) {
            log.debug("User rejected last-seen PATCH for {} ({}): not retrying", userId, e.getStatusCode());
        } catch (Exception e) {
            if (mayRetry) {
                log.debug("Retrying last-seen PATCH for {} once", userId);
                attempt(userId, seenAt, false);
            } else {
                log.debug("last-seen PATCH for {} failed after retry; presence degrades silently", userId);
            }
        }
    }
}
