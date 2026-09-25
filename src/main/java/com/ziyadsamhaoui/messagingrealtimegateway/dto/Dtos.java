package com.ziyadsamhaoui.messagingrealtimegateway.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public final class Dtos {

    private Dtos() {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record SendMessageRequest(
            @NotBlank String roomId,
            @NotBlank String type,
            @NotBlank String content
    ) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record TypingRequest(
            @NotNull String roomId
    ) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record ErrorFrame(
            String timestamp,
            int status,
            String code,
            String message,
            String destination
    ) {
        public static ErrorFrame of(int status, String code, String message, String destination) {
            return new ErrorFrame(java.time.Instant.now().toString(), status, code, message, destination);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ChatErrorBody(
            Integer status,
            String code,
            String message
    ) {
    }

    public record LastSeenRequest(
            java.time.Instant seenAt
    ) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ChatRoomView(
            String id
    ) {
    }
}
