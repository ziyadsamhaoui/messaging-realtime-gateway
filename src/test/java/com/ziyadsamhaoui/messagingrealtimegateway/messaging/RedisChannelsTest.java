package com.ziyadsamhaoui.messagingrealtimegateway.messaging;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RedisChannelsTest {

    @Test
    void messageAndTypingPatternsDoNotOverlap() {
        String messages = RedisChannels.messagesPatternTopic().getTopic();
        String typing = RedisChannels.typingPatternTopic().getTopic();

        assertThat(messages).isEqualTo("room:*:messages");
        assertThat(typing).isEqualTo("room:*:typing");

        assertThat(matches(messages, "room:42:messages")).isTrue();
        assertThat(matches(typing, "room:42:messages")).isFalse();

        assertThat(matches(typing, "room:42:typing")).isTrue();
        assertThat(matches(messages, "room:42:typing")).as("typing must match exactly one pattern").isFalse();
    }

    @Test
    void channelsAndDestinationsRoundTrip() {
        assertThat(RedisChannels.messagesChannel("42")).isEqualTo("room:42:messages");
        assertThat(RedisChannels.typingChannel("42")).isEqualTo("room:42:typing");

        assertThat(RedisChannels.destinationForChannel("room:42:messages")).isEqualTo("/topic/rooms/42");
        assertThat(RedisChannels.destinationForChannel("room:42:typing")).isEqualTo("/topic/rooms/42/typing");

        assertThat(RedisChannels.destinationForChannel("presence:user-1")).isNull();
        assertThat(RedisChannels.destinationForChannel(null)).isNull();
    }

    private static boolean matches(String pattern, String channel) {
        return channel.matches(pattern.replace("*", ".*"));
    }
}
