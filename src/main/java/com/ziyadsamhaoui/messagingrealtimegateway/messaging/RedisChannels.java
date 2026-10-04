package com.ziyadsamhaoui.messagingrealtimegateway.messaging;

import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.Topic;

public final class RedisChannels {

    public static final String ROOM_CHANNEL_PREFIX = "room:";
    public static final String TYPING_CHANNEL_SUFFIX = ":typing";
    public static final String TYPING_DESTINATION_PREFIX = "/topic/rooms/";
    public static final String TYPING_DESTINATION_SUFFIX = "/typing";

    private static final Topic TYPING_PATTERN_TOPIC =
            new PatternTopic(ROOM_CHANNEL_PREFIX + "*" + TYPING_CHANNEL_SUFFIX);

    private RedisChannels() {
    }

    public static Topic typingPatternTopic() {
        return TYPING_PATTERN_TOPIC;
    }

    public static String typingChannel(String roomId) {
        return ROOM_CHANNEL_PREFIX + roomId + TYPING_CHANNEL_SUFFIX;
    }

    public static String destinationForChannel(String channel) {
        if (channel != null && channel.startsWith(ROOM_CHANNEL_PREFIX)
                && channel.endsWith(TYPING_CHANNEL_SUFFIX)) {
            return TYPING_DESTINATION_PREFIX + channel.substring(ROOM_CHANNEL_PREFIX.length(),
                    channel.length() - TYPING_CHANNEL_SUFFIX.length()) + TYPING_DESTINATION_SUFFIX;
        }
        return null;
    }
}
