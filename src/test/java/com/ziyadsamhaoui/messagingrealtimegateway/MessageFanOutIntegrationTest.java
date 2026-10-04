package com.ziyadsamhaoui.messagingrealtimegateway;

import com.ziyadsamhaoui.messagingrealtimegateway.service.MessageFanOutService;
import com.ziyadsamhaoui.messagingrealtimegateway.support.AbstractIntegrationTest;
import com.ziyadsamhaoui.messagingrealtimegateway.support.StompTestClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class MessageFanOutIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    MessageFanOutService fanOutService;

    @Autowired
    StringRedisTemplate redis;

    @Autowired
    ObjectMapper objectMapper;

    private StompTestClient subscriber(String userId, String destination) throws InterruptedException {
        StompTestClient client = new StompTestClient();
        String token = TOKENS.mint(userId);
        boolean ok = client.connect("ws://localhost:" + port() + "/ws/websocket",
                h -> h.add("Authorization", "Bearer " + token), 5);
        if (!ok) {
            client.close();
            throw new IllegalStateException("client failed to connect: " + client.connectError());
        }
        client.subscribe(destination);
        Thread.sleep(250);
        return client;
    }

    private JsonNode payload(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Test
    void messageSentEventBroadcastsToRoomSubscribers() throws Exception {
        try (StompTestClient viewer = subscriber("viewer-1", "/topic/rooms/42")) {
            fanOutService.fanOut("fanout-basic", "MESSAGE_SENT", payload("""
                    {"messageId":"m1","roomId":"42","senderId":"sender-1","type":"TEXT",
                     "content":"hello","createdAt":"2026-10-04T12:00:00Z"}
                    """));

            String frame = viewer.nextFrame("/topic/rooms/42", 5);
            assertThat(frame).isNotNull();
            assertThat(frame).contains("\"messageId\":\"m1\"");
            assertThat(frame).contains("\"senderId\":\"sender-1\"");
            assertThat(frame).contains("\"content\":\"hello\"");
        }
    }

    @Test
    void duplicateEventBroadcastsExactlyOnceAndClaimsWithTtl() throws Exception {
        try (StompTestClient viewer = subscriber("viewer-2", "/topic/rooms/42")) {
            fanOutService.fanOut("fanout-dup", "MESSAGE_SENT",
                    payload("{\"messageId\":\"m2\",\"roomId\":\"42\",\"senderId\":\"s\",\"content\":\"once\"}"));
            fanOutService.fanOut("fanout-dup", "MESSAGE_SENT",
                    payload("{\"messageId\":\"m2\",\"roomId\":\"42\",\"senderId\":\"s\",\"content\":\"once\"}"));

            assertThat(viewer.nextFrame("/topic/rooms/42", 5)).isNotNull();
            assertThat(viewer.nextFrame("/topic/rooms/42", 1)).isNull();
        }

        assertThat(redis.hasKey("message_dedup:fanout-dup")).isTrue();
        Long ttl = redis.getExpire("message_dedup:fanout-dup", TimeUnit.SECONDS);
        assertThat(ttl).isPositive();
        assertThat(ttl).isLessThanOrEqualTo(60);
    }

    @Test
    void invitationSentBroadcastsToTheInvitedUsersTopic() throws Exception {
        try (StompTestClient invited = subscriber("invited-1", "/topic/users/invited-1")) {
            fanOutService.fanOut("fanout-invite", "INVITATION_SENT", payload("""
                    {"invitationId":"i1","roomId":"42","invitedId":"invited-1","inviterId":"inviter-1",
                     "roomName":"Team","inviterUsername":"alice","sentAt":"2026-10-04T12:00:00Z"}
                    """));

            String frame = invited.nextFrame("/topic/users/invited-1", 5);
            assertThat(frame).isNotNull();
            assertThat(frame).contains("\"invitationId\":\"i1\"");
            assertThat(frame).contains("\"inviterUsername\":\"alice\"");
        }
    }

    @Test
    void invitationAcceptedBroadcastsToInviterAndRoom() throws Exception {
        try (StompTestClient inviter = subscriber("inviter-2", "/topic/users/inviter-2");
             StompTestClient room = subscriber("member-2", "/topic/rooms/77")) {
            fanOutService.fanOut("fanout-accept", "INVITATION_ACCEPTED", payload("""
                    {"invitationId":"i2","roomId":"77","invitedId":"invited-2","inviterId":"inviter-2",
                     "invitedUsername":"bob","acceptedAt":"2026-10-04T12:00:00Z"}
                    """));

            assertThat(inviter.nextFrame("/topic/users/inviter-2", 5)).isNotNull();
            assertThat(room.nextFrame("/topic/rooms/77", 5)).isNotNull();
        }
    }
}
