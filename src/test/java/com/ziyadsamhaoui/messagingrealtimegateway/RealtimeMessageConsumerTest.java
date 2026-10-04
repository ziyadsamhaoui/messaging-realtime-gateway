package com.ziyadsamhaoui.messagingrealtimegateway;

import com.ziyadsamhaoui.messagingrealtimegateway.messaging.RealtimeMessageConsumer;
import com.ziyadsamhaoui.messagingrealtimegateway.service.MessageFanOutService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class RealtimeMessageConsumerTest {

    private final MessageFanOutService fanOutService = mock(MessageFanOutService.class);
    private final RealtimeMessageConsumer consumer =
            new RealtimeMessageConsumer(fanOutService, new ObjectMapper());

    private static ConsumerRecord<String, String> record(String value) {
        return new ConsumerRecord<>(RealtimeMessageConsumer.TOPIC, 0, 0L, "42", value);
    }

    @Test
    void messageSentIsDelegatedToFanOut() {
        consumer.onMessage(record("""
                {"eventId":"e1","eventType":"MESSAGE_SENT","eventVersion":1,
                 "occurredAt":"2026-10-04T12:00:00Z","producer":"messaging-chat-service",
                 "correlationId":null,"aggregateId":"42","payload":
                 {"messageId":"m1","roomId":"42","senderId":"u1","senderUsername":"alice",
                  "type":"TEXT","content":"hello","createdAt":"2026-10-04T12:00:00Z"}}
                """));

        verify(fanOutService).fanOut("m1", "42", "u1", "TEXT", "hello");
    }

    @Test
    void nonMessageSentEventsAreIgnored() {
        consumer.onMessage(record("{\"eventType\":\"MESSAGE_DELETED\",\"payload\":{\"messageId\":\"m1\"}}"));

        verifyNoInteractions(fanOutService);
    }

    @Test
    void malformedRecordsAreContained() {
        consumer.onMessage(record("{not-json"));
        consumer.onMessage(record("{\"eventType\":\"MESSAGE_SENT\"}"));
        consumer.onMessage(record("{\"eventType\":\"MESSAGE_SENT\",\"payload\":null}"));

        verifyNoInteractions(fanOutService);
    }
}
