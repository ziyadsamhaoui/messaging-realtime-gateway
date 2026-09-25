package com.ziyadsamhaoui.messagingrealtimegateway.config;

import tools.jackson.databind.json.JsonMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.time.Duration;

@Configuration
public class ChatServiceClientConfig {

    @Bean
    public RestClient chatServiceRestClient(
            @Value("${upstream.chat-service}") String baseUrl,
            @Value("${realtime.connect-timeout-ms}") int connectTimeoutMs,
            @Value("${realtime.read-timeout-ms}") int readTimeoutMs,
            JsonMapper jsonMapper) {
        return RestClient.builder()
                .baseUrl(baseUrl)
                .requestFactory(clientHttpRequestFactory(connectTimeoutMs, readTimeoutMs))
                .messageConverters(converters -> converters.addFirst(
                        new org.springframework.http.converter.json.JacksonJsonHttpMessageConverter(jsonMapper)))
                .build();
    }

    static SimpleClientHttpRequestFactory clientHttpRequestFactory(int connectTimeoutMs, int readTimeoutMs) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(Duration.ofMillis(connectTimeoutMs));
        factory.setReadTimeout(Duration.ofMillis(readTimeoutMs));
        return factory;
    }
}
