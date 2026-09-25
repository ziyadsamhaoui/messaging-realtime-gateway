package com.ziyadsamhaoui.messagingrealtimegateway.stomp;

import org.springframework.stereotype.Component;

import java.security.Principal;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class StompSessionRegistry {

    private final Map<String, Principal> sessions = new ConcurrentHashMap<>();

    public void register(String sessionId, Principal user) {
        sessions.put(sessionId, user);
    }

    public void unregister(String sessionId) {
        sessions.remove(sessionId);
    }

    public Principal getUser(String sessionId) {
        return sessions.get(sessionId);
    }

    public int localSessionCount() {
        return sessions.size();
    }

    public Set<String> sessionIds() {
        return Set.copyOf(sessions.keySet());
    }
}
