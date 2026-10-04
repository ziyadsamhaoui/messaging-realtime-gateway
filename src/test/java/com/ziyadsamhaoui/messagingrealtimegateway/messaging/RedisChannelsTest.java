package com.ziyadsamhaoui.messagingrealtimegateway.messaging;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RedisChannelsTest {

    @Test
    void typingPatternMatchesOnlyTypingChannels() {
        String typing = RedisChannels.typingPatternTopic().getTopic();

        assertThat(typing).isEqualTo("room:*:typing");
        assertThat(matches(typing, "room:42:typing")).isTrue();
        assertThat(matches(typing, "room:42:messages")).isFalse();
    }

    @Test
    void typingChannelAndDestinationRoundTrip() {
        assertThat(RedisChannels.typingChannel("42")).isEqualTo("room:42:typing");
        assertThat(RedisChannels.destinationForChannel("room:42:typing")).isEqualTo("/topic/rooms/42/typing");
        assertThat(RedisChannels.destinationForChannel("room:42:messages")).isNull();
        assertThat(RedisChannels.destinationForChannel("presence:user-1")).isNull();
        assertThat(RedisChannels.destinationForChannel(null)).isNull();
    }

    private static boolean matches(String pattern, String channel) {
        return channel.matches(pattern.replace("*", ".*"));
    }
}
