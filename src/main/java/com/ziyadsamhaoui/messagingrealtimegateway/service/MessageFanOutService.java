package com.ziyadsamhaoui.messagingrealtimegateway.service;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;

@Service
public class MessageFanOutService {

    public static final String DEDUP_KEY_PREFIX = "message_dedup:";
    public static final Duration DEDUP_TTL = Duration.ofSeconds(60);
    public static final String ROOM_TOPIC_PREFIX = "/topic/rooms/";
    public static final String USER_TOPIC_PREFIX = "/topic/users/";

    public static final String MESSAGE_SENT = "MESSAGE_SENT";
    public static final String REACTION_ADDED = "REACTION_ADDED";
    public static final String MESSAGE_DELETED = "MESSAGE_DELETED";
    public static final String PARTICIPANT_ADDED = "PARTICIPANT_ADDED";
    public static final String PARTICIPANT_REMOVED = "PARTICIPANT_REMOVED";
    public static final String PARTICIPANT_MUTED = "PARTICIPANT_MUTED";
    public static final String PARTICIPANT_UNMUTED = "PARTICIPANT_UNMUTED";
    public static final String INVITATION_SENT = "INVITATION_SENT";
    public static final String INVITATION_ACCEPTED = "INVITATION_ACCEPTED";
    public static final String USER_CONNECTION_ACCEPTED = "USER_CONNECTION_ACCEPTED";

    private static final Logger log = LoggerFactory.getLogger(MessageFanOutService.class);

    private final StringRedisTemplate redis;
    private final SimpMessagingTemplate messagingTemplate;
    private final ObjectMapper objectMapper;

    public MessageFanOutService(StringRedisTemplate redis,
                                SimpMessagingTemplate messagingTemplate,
                                ObjectMapper objectMapper) {
        this.redis = redis;
        this.messagingTemplate = messagingTemplate;
        this.objectMapper = objectMapper;
    }

    public void fanOut(String eventId, String eventType, JsonNode payload) {
        if (eventId == null || eventId.isBlank() || eventType == null || payload == null) {
            log.error("Discarding incomplete event (eventId={}, eventType={})", eventId, eventType);
            return;
        }
        if (!claim(eventId)) {
            log.debug("Duplicate {} event {} skipped; another node or execution already broadcast it",
                    eventType, eventId);
            return;
        }
        try {
            dispatch(eventType, payload);
        } catch (RuntimeException e) {
            log.error("Failed to dispatch {} event {}: {}", eventType, eventId, e.getMessage());
        }
    }

    private void dispatch(String eventType, JsonNode payload) {
        switch (eventType) {
            case MESSAGE_SENT -> broadcast(roomDestination(payload), new RoomMessageFrame(
                    text(payload, "messageId"),
                    text(payload, "senderId"),
                    text(payload, "type"),
                    text(payload, "content"),
                    text(payload, "createdAt")));
            case REACTION_ADDED -> broadcast(roomDestination(payload), new ReactionFrame(
                    text(payload, "messageId"),
                    text(payload, "userId"),
                    text(payload, "reactorUsername"),
                    text(payload, "emoji")));
            case MESSAGE_DELETED -> broadcast(roomDestination(payload), new MessageDeletedFrame(
                    text(payload, "messageId"),
                    text(payload, "roomId")));
            case PARTICIPANT_ADDED -> broadcast(roomDestination(payload), new ParticipantFrame(
                    eventType,
                    text(payload, "roomId"),
                    text(payload, "userId"),
                    text(payload, "role"),
                    null,
                    null));
            case PARTICIPANT_REMOVED -> broadcast(roomDestination(payload), new ParticipantFrame(
                    eventType,
                    text(payload, "roomId"),
                    text(payload, "userId"),
                    null,
                    null,
                    null));
            case PARTICIPANT_MUTED -> broadcast(roomDestination(payload), new ParticipantFrame(
                    eventType,
                    text(payload, "roomId"),
                    text(payload, "userId"),
                    null,
                    flag(payload, "muted"),
                    text(payload, "mutedUntil")));
            case PARTICIPANT_UNMUTED -> broadcast(roomDestination(payload), new ParticipantFrame(
                    eventType,
                    text(payload, "roomId"),
                    text(payload, "userId"),
                    null,
                    null,
                    null));
            case INVITATION_SENT -> broadcast(userDestination(text(payload, "invitedId")), new InvitationSentFrame(
                    eventType,
                    text(payload, "invitationId"),
                    text(payload, "roomId"),
                    text(payload, "invitedId"),
                    text(payload, "inviterId"),
                    text(payload, "roomName"),
                    text(payload, "inviterUsername"),
                    text(payload, "sentAt")));
            case INVITATION_ACCEPTED -> {
                InvitationAcceptedFrame frame = new InvitationAcceptedFrame(
                        eventType,
                        text(payload, "invitationId"),
                        text(payload, "roomId"),
                        text(payload, "invitedId"),
                        text(payload, "inviterId"),
                        text(payload, "invitedUsername"),
                        text(payload, "acceptedAt"));
                broadcast(userDestination(text(payload, "inviterId")), frame);
                broadcast(roomDestination(payload), frame);
            }
            case USER_CONNECTION_ACCEPTED -> {
                ConnectionAcceptedFrame frame = new ConnectionAcceptedFrame(
                        eventType,
                        text(payload, "userIdA"),
                        text(payload, "userIdB"),
                        text(payload, "userAUsername"),
                        text(payload, "userBUsername"),
                        text(payload, "acceptedAt"));
                broadcast(userDestination(text(payload, "userIdA")), frame);
                broadcast(userDestination(text(payload, "userIdB")), frame);
            }
            default -> log.debug("Ignoring {} event; no realtime fan-out is defined", eventType);
        }
    }

    private void broadcast(String destination, Object frame) {
        if (destination == null) {
            log.error("Skipping {} frame with no resolvable destination", frame.getClass().getSimpleName());
            return;
        }
        try {
            String body = objectMapper.writeValueAsString(frame);
            messagingTemplate.convertAndSend(destination, body);
            log.debug("Broadcast {} to {}", frame.getClass().getSimpleName(), destination);
        } catch (Exception e) {
            log.error("Failed to broadcast to {}: {}", destination, e.getMessage());
        }
    }

    private boolean claim(String eventId) {
        try {
            Boolean first = redis.opsForValue().setIfAbsent(DEDUP_KEY_PREFIX + eventId, "1", DEDUP_TTL);
            return Boolean.TRUE.equals(first);
        } catch (DataAccessException e) {
            log.error("Deduplication unavailable for event {}; broadcasting without deduplication: {}",
                    eventId, e.getMessage());
            return true;
        }
    }

    private static String roomDestination(JsonNode payload) {
        String roomId = text(payload, "roomId");
        return roomId == null || roomId.isBlank() ? null : ROOM_TOPIC_PREFIX + roomId;
    }

    private static String userDestination(String userId) {
        return userId == null || userId.isBlank() ? null : USER_TOPIC_PREFIX + userId;
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asString();
    }

    private static Boolean flag(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asBoolean();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record RoomMessageFrame(String messageId, String senderId, String type, String content, String createdAt) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record ReactionFrame(String messageId, String reactorId, String reactorUsername, String emoji) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record MessageDeletedFrame(String messageId, String roomId) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record ParticipantFrame(String eventType, String roomId, String userId, String role, Boolean muted,
                                   String mutedUntil) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record InvitationSentFrame(String eventType, String invitationId, String roomId, String invitedId,
                                      String inviterId, String roomName, String inviterUsername, String sentAt) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record InvitationAcceptedFrame(String eventType, String invitationId, String roomId, String invitedId,
                                          String inviterId, String invitedUsername, String acceptedAt) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record ConnectionAcceptedFrame(String eventType, String userIdA, String userIdB, String userAUsername,
                                          String userBUsername, String acceptedAt) {
    }
}
