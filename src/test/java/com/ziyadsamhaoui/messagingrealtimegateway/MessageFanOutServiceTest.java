package com.ziyadsamhaoui.messagingrealtimegateway;

import com.ziyadsamhaoui.messagingrealtimegateway.service.MessageFanOutService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.messaging.simp.SimpMessagingTemplate;
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
        service = new MessageFanOutService(redis, messagingTemplate, new ObjectMapper());
    }

    @Test
    @SuppressWarnings("unchecked")
    void firstMessageIsBroadcastToTheRoomTopic() {
        when(values.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(true);

        service.fanOut("m1", "42", "user-1", "TEXT", "hello");

        ArgumentCaptor<String> destination = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Object> frame = ArgumentCaptor.forClass(Object.class);
        verify(messagingTemplate).convertAndSend(destination.capture(), frame.capture());
        assertThat(destination.getValue()).isEqualTo("/topic/rooms/42");
        assertThat(frame.getValue().toString())
                .contains("\"senderId\":\"user-1\"")
                .contains("\"type\":\"TEXT\"")
                .contains("\"content\":\"hello\"");
        verify(values).setIfAbsent(eq("message_dedup:m1"), eq("1"), eq(Duration.ofSeconds(60)));
    }

    @Test
    void duplicateMessageIdIsBroadcastOnlyOnce() {
        when(values.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(true, false);

        service.fanOut("m1", "42", "user-1", "TEXT", "hello");
        service.fanOut("m1", "42", "user-1", "TEXT", "hello");

        verify(messagingTemplate, times(1)).convertAndSend(anyString(), any(Object.class));
    }

    @Test
    void broadcastFailsOpenWhenDeduplicationIsUnavailable() {
        when(values.setIfAbsent(anyString(), anyString(), any(Duration.class)))
                .thenThrow(new RedisConnectionFailureException("down"));

        service.fanOut("m1", "42", "user-1", "TEXT", "hello");

        verify(messagingTemplate).convertAndSend(eq("/topic/rooms/42"), any(Object.class));
    }

    @Test
    void incompleteEventIsDiscardedWithoutBroadcast() {
        service.fanOut(null, "42", "user-1", "TEXT", "hello");
        service.fanOut("m1", " ", "user-1", "TEXT", "hello");

        verifyNoInteractions(messagingTemplate);
    }
}
