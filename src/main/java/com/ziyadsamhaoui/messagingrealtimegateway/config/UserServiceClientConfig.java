package com.ziyadsamhaoui.messagingrealtimegateway.config;

import tools.jackson.databind.json.JsonMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.time.Duration;

@Configuration
public class UserServiceClientConfig {

    @Bean
    public RestClient userServiceRestClient(
            @Value("${upstream.user-service}") String baseUrl,
            @Value("${upstream.user-service-internal-token}") String internalToken,
            @Value("${realtime.connect-timeout-ms}") int connectTimeoutMs,
            @Value("${realtime.read-timeout-ms}") int readTimeoutMs,
            JsonMapper jsonMapper) {
        return RestClient.builder()
                .baseUrl(baseUrl)
                .requestFactory(ChatServiceClientConfig.clientHttpRequestFactory(connectTimeoutMs, readTimeoutMs))
                .defaultHeader("X-Internal-Token", internalToken)
                .messageConverters(converters -> converters.addFirst(
                        new org.springframework.http.converter.json.JacksonJsonHttpMessageConverter(jsonMapper)))
                .build();
    }
}
