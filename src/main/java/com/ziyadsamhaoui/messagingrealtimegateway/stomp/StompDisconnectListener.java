package com.ziyadsamhaoui.messagingrealtimegateway.stomp;

import com.ziyadsamhaoui.messagingrealtimegateway.service.PresenceService;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.messaging.SessionDisconnectEvent;

@Component
public class StompDisconnectListener implements ApplicationListener<SessionDisconnectEvent> {

    private final PresenceService presenceService;

    public StompDisconnectListener(PresenceService presenceService) {
        this.presenceService = presenceService;
    }

    @Override
    public void onApplicationEvent(SessionDisconnectEvent event) {
        var user = event.getUser();
        if (user != null) {
            presenceService.onDisconnect(user.getName());
        }
    }
}
