package com.ziyadsamhaoui.messagingrealtimegateway;

import com.ziyadsamhaoui.messagingrealtimegateway.service.MessageFanOutService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class MessageFanOutServiceTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private StringRedisTemplate redis;
    private ValueOperations<String, String> values;
    private SimpMessagingTemplate messagingTemplate;
    private MessageFanOutService service;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redis = mock(StringRedisTemplate.class);
        values = mock(ValueOperations.class);
        messagingTemplate = mock(SimpMessagingTemplate.class);
        when(redis.opsForValue()).thenReturn(values);
        service = new MessageFanOutService(redis, messagingTemplate, objectMapper);
    }

    private JsonNode payload(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Test
    void winningClaimBroadcastsMessageToTheRoomTopic() {
        when(values.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(true);

        service.fanOut("evt-1", "MESSAGE_SENT", payload("""
                {"messageId":"m1","roomId":"42","senderId":"user-1","senderUsername":"alice",
                 "type":"TEXT","content":"hello","createdAt":"2026-10-04T12:00:00Z"}
                """));

        ArgumentCaptor<String> destination = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Object> frame = ArgumentCaptor.forClass(Object.class);
        verify(messagingTemplate).convertAndSend(destination.capture(), frame.capture());
        assertThat(destination.getValue()).isEqualTo("/topic/rooms/42");
        assertThat(frame.getValue().toString())
                .contains("\"messageId\":\"m1\"")
                .contains("\"senderId\":\"user-1\"")
                .contains("\"type\":\"TEXT\"")
                .contains("\"content\":\"hello\"");
        verify(values).setIfAbsent(eq("message_dedup:evt-1"), eq("1"), eq(Duration.ofSeconds(60)));
    }

    @Test
    void attachmentUpdateIsBroadcastToTheRoomTopic() {
        when(values.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(true);

        service.fanOut("evt-2", "MESSAGE_ATTACHMENTS_UPDATED", payload("""
                {"messageId":"m1","roomId":"42","attachmentIds":["a1","a2"]}
                """));

        ArgumentCaptor<String> destination = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Object> frame = ArgumentCaptor.forClass(Object.class);
        verify(messagingTemplate).convertAndSend(destination.capture(), frame.capture());
        assertThat(destination.getValue()).isEqualTo("/topic/rooms/42");
        assertThat(frame.getValue().toString())
                .contains("\"eventType\":\"MESSAGE_ATTACHMENTS_UPDATED\"")
                .contains("\"messageId\":\"m1\"")
                .contains("\"attachmentIds\":[\"a1\",\"a2\"]");
    }

    @Test
    void losingClaimIsSkippedWithoutBroadcast() {
        when(values.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(true, false);

        service.fanOut("evt-1", "MESSAGE_SENT", payload("{\"roomId\":\"42\",\"content\":\"a\"}"));
        service.fanOut("evt-1", "MESSAGE_SENT", payload("{\"roomId\":\"42\",\"content\":\"a\"}"));

        verify(messagingTemplate, times(1)).convertAndSend(anyString(), any(Object.class));
    }

    @Test
    void redisFailureFailsOpenWithBroadcast() {
        when(values.setIfAbsent(anyString(), anyString(), any(Duration.class)))
                .thenThrow(new RedisConnectionFailureException("down"));

        service.fanOut("evt-1", "MESSAGE_SENT", payload("{\"roomId\":\"42\",\"content\":\"a\"}"));

        verify(messagingTemplate).convertAndSend(eq("/topic/rooms/42"), any(Object.class));
    }

    @Test
    void eventTypesRouteToTheirTargetTopics() {
        when(values.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(true);

        service.fanOut("e1", "REACTION_ADDED",
                payload("{\"messageId\":\"m1\",\"roomId\":\"42\",\"userId\":\"u1\",\"reactorUsername\":\"bob\",\"emoji\":\"+1\"}"));
        service.fanOut("e2", "PARTICIPANT_MUTED",
                payload("{\"roomId\":\"42\",\"userId\":\"u1\",\"muted\":true,\"mutedUntil\":\"2026-10-05T00:00:00Z\"}"));
        service.fanOut("e3", "INVITATION_SENT",
                payload("{\"invitationId\":\"i1\",\"roomId\":\"42\",\"invitedId\":\"u2\",\"inviterId\":\"u1\"}"));
        service.fanOut("e4", "INVITATION_ACCEPTED",
                payload("{\"invitationId\":\"i1\",\"roomId\":\"42\",\"invitedId\":\"u2\",\"inviterId\":\"u1\"}"));
        service.fanOut("e5", "USER_CONNECTION_ACCEPTED",
                payload("{\"userIdA\":\"u1\",\"userIdB\":\"u2\"}"));

        ArgumentCaptor<String> destination = ArgumentCaptor.forClass(String.class);
        verify(messagingTemplate, times(7)).convertAndSend(destination.capture(), any(Object.class));
        assertThat(destination.getAllValues()).containsExactly(
                "/topic/rooms/42",
                "/topic/rooms/42",
                "/topic/users/u2",
                "/topic/users/u1",
                "/topic/rooms/42",
                "/topic/users/u1",
                "/topic/users/u2");
    }

    @Test
    void unknownEventTypeIsIgnored() {
        when(values.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(true);

        service.fanOut("e1", "SOMETHING_ELSE", payload("{\"roomId\":\"42\"}"));

        verifyNoInteractions(messagingTemplate);
    }
}
