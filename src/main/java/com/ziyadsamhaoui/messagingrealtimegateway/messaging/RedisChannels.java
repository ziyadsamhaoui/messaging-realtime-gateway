package com.ziyadsamhaoui.messagingrealtimegateway.messaging;

import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.Topic;

public final class RedisChannels {

    public static final String MESSAGES_CHANNEL_PREFIX = "room:";
    public static final String MESSAGES_CHANNEL_SUFFIX = ":messages";
    public static final String TYPING_CHANNEL_SUFFIX = ":typing";

    private static final Topic MESSAGES_PATTERN_TOPIC =
            new PatternTopic(MESSAGES_CHANNEL_PREFIX + "*" + MESSAGES_CHANNEL_SUFFIX);
    private static final Topic TYPING_PATTERN_TOPIC = new PatternTopic(MESSAGES_CHANNEL_PREFIX + "*" + TYPING_CHANNEL_SUFFIX);

    private RedisChannels() {
    }

    public static Topic messagesPatternTopic() {
        return MESSAGES_PATTERN_TOPIC;
    }

    public static Topic typingPatternTopic() {
        return TYPING_PATTERN_TOPIC;
    }

    static Topic messageChannelTopic() {
        return messagesPatternTopic();
    }

    static Topic typingChannelTopic() {
        return typingPatternTopic();
    }

    public static String messagesChannel(String roomId) {
        return MESSAGES_CHANNEL_PREFIX + roomId + MESSAGES_CHANNEL_SUFFIX;
    }

    public static String typingChannel(String roomId) {
        return MESSAGES_CHANNEL_PREFIX + roomId + TYPING_CHANNEL_SUFFIX;
    }

    public static String destinationForChannel(String channel) {
        if (channel != null && channel.startsWith(MESSAGES_CHANNEL_PREFIX) && channel.endsWith(MESSAGES_CHANNEL_SUFFIX)) {
            return "/topic/rooms/" + channel.substring(MESSAGES_CHANNEL_PREFIX.length(),
                    channel.length() - MESSAGES_CHANNEL_SUFFIX.length());
        }
        if (channel != null && channel.startsWith(MESSAGES_CHANNEL_PREFIX) && channel.endsWith(TYPING_CHANNEL_SUFFIX)) {
            return "/topic/rooms/" + channel.substring(MESSAGES_CHANNEL_PREFIX.length(),
                    channel.length() - TYPING_CHANNEL_SUFFIX.length()) + "/typing";
        }
        return null;
    }
}
