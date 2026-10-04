package com.ziyadsamhaoui.messagingrealtimegateway;

import com.ziyadsamhaoui.messagingrealtimegateway.messaging.RealtimeMessageConsumer;
import com.ziyadsamhaoui.messagingrealtimegateway.service.MessageFanOutService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class RealtimeMessageConsumerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final MessageFanOutService fanOutService = mock(MessageFanOutService.class);
    private final RealtimeMessageConsumer consumer =
            new RealtimeMessageConsumer(fanOutService, objectMapper);

    private static ConsumerRecord<String, String> record(String topic, String value) {
        return new ConsumerRecord<>(topic, 0, 0L, "42", value);
    }

    @Test
    void messageSentEnvelopeIsParsedAndDelegated() {
        consumer.onMessage(record(RealtimeMessageConsumer.MESSAGE_TOPIC, """
                {"eventId":"e1","eventType":"MESSAGE_SENT","eventVersion":1,
                 "occurredAt":"2026-10-04T12:00:00Z","producer":"messaging-chat-service",
                 "correlationId":null,"aggregateId":"42","payload":
                 {"messageId":"m1","roomId":"42","senderId":"u1","senderUsername":"alice",
                  "type":"TEXT","content":"hello","createdAt":"2026-10-04T12:00:00Z"}}
                """));

        verify(fanOutService).fanOut(eq("e1"), eq("MESSAGE_SENT"), any(JsonNode.class));
    }

    @Test
    void reactionAndParticipantEventsAreDelegated() {
        consumer.onMessage(record(RealtimeMessageConsumer.MESSAGE_TOPIC, """
                {"eventId":"e2","eventType":"REACTION_ADDED","payload":{"messageId":"m1","roomId":"42",
                 "userId":"u2","reactorUsername":"bob","emoji":"+1"}}
                """));
        consumer.onMessage(record(RealtimeMessageConsumer.ROOM_TOPIC, """
                {"eventId":"e3","eventType":"PARTICIPANT_MUTED","payload":{"roomId":"42","userId":"u2",
                 "muted":true,"mutedUntil":"2026-10-05T00:00:00Z"}}
                """));

        verify(fanOutService).fanOut(eq("e2"), eq("REACTION_ADDED"), any(JsonNode.class));
        verify(fanOutService).fanOut(eq("e3"), eq("PARTICIPANT_MUTED"), any(JsonNode.class));
    }

    @Test
    void invitationAndConnectionEventsAreDelegated() {
        consumer.onMessage(record(RealtimeMessageConsumer.INVITATION_TOPIC, """
                {"eventId":"e4","eventType":"INVITATION_SENT","payload":{"invitationId":"i1","roomId":"42",
                 "invitedId":"u2","inviterId":"u1","roomName":"Team","inviterUsername":"alice",
                 "sentAt":"2026-10-04T12:00:00Z"}}
                """));
        consumer.onMessage(record(RealtimeMessageConsumer.PROFILE_TOPIC, """
                {"eventId":"e5","eventType":"USER_CONNECTION_ACCEPTED","payload":{"userIdA":"u1",
                 "userIdB":"u2","acceptedAt":"2026-10-04T12:00:00Z"}}
                """));

        verify(fanOutService).fanOut(eq("e4"), eq("INVITATION_SENT"), any(JsonNode.class));
        verify(fanOutService).fanOut(eq("e5"), eq("USER_CONNECTION_ACCEPTED"), any(JsonNode.class));
    }

    @Test
    void incompleteOrMalformedRecordsAreContained() {
        consumer.onMessage(record(RealtimeMessageConsumer.MESSAGE_TOPIC, "{not-json"));
        consumer.onMessage(record(RealtimeMessageConsumer.MESSAGE_TOPIC, "{\"eventType\":\"MESSAGE_SENT\"}"));
        consumer.onMessage(record(RealtimeMessageConsumer.MESSAGE_TOPIC, "{\"eventId\":\"e9\",\"eventType\":\"MESSAGE_SENT\",\"payload\":null}"));

        verifyNoInteractions(fanOutService);
    }
}
