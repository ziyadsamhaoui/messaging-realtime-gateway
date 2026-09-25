package com.ziyadsamhaoui.messagingrealtimegateway.client;

import com.ziyadsamhaoui.messagingrealtimegateway.dto.Dtos;
import com.ziyadsamhaoui.messagingrealtimegateway.exception.ChatServiceException;
import com.ziyadsamhaoui.messagingrealtimegateway.exception.UpstreamUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import tools.jackson.databind.ObjectMapper;

@Component
public class ChatServiceClient {

    private static final Logger log = LoggerFactory.getLogger(ChatServiceClient.class);

    private final RestClient restClient;
    private final ObjectMapper objectMapper;

    public ChatServiceClient(RestClient chatServiceRestClient, ObjectMapper objectMapper) {
        this.restClient = chatServiceRestClient;
        this.objectMapper = objectMapper;
    }

    public void getRoom(String roomId, String authorization) {
        try {
            restClient.get()
                    .uri("/rooms/{roomId}", roomId)
                    .header("Authorization", authorization)
                    .retrieve()
                    .toBodilessEntity();
        } catch (org.springframework.web.client.HttpClientErrorException
                 | org.springframework.web.client.HttpServerErrorException e) {
            throw relayed(e.getStatusCode(), e.getResponseBodyAsString());
        } catch (RestClientException e) {
            log.error("Chat GET /rooms/{} unreachable: {}", roomId, e.getMessage());
            throw new UpstreamUnavailableException("Chat service unavailable", e);
        }
    }

    public void sendMessage(String roomId, String authorization, Dtos.SendMessageRequest request) {
        try {
            restClient.post()
                    .uri("/rooms/{roomId}/messages", roomId)
                    .header("Authorization", authorization)
                    .body(request)
                    .retrieve()
                    .toBodilessEntity();
        } catch (org.springframework.web.client.HttpClientErrorException
                 | org.springframework.web.client.HttpServerErrorException e) {
            throw relayed(e.getStatusCode(), e.getResponseBodyAsString());
        } catch (RestClientException e) {
            log.error("Chat POST /rooms/{}/messages unreachable: {}", roomId, e.getMessage());
            throw new UpstreamUnavailableException("Chat service unavailable", e);
        }
    }

    private ChatServiceException relayed(HttpStatusCode status, String body) {
        if (status.is5xxServerError() && status.value() != 503) {
            log.error("Chat returned unexpected {}", status.value());
            throw new UpstreamUnavailableException("Chat service failed (HTTP " + status.value() + ")");
        }
        String code = "UPSTREAM_ERROR";
        String message = "Chat rejected the request";
        try {
            Dtos.ChatErrorBody parsed = objectMapper.readValue(body == null ? "" : body, Dtos.ChatErrorBody.class);
            if (parsed.code() != null) {
                code = parsed.code();
            }
            if (parsed.message() != null) {
                message = parsed.message();
            }
        } catch (Exception parseFailure) {
            log.debug("Could not parse Chat error body; relaying generic code");
        }
        log.debug("Chat rejected with {}: {}", status.value(), code);
        return new ChatServiceException(status.value(), code, message);
    }
}
