package com.ziyadsamhaoui.messagingrealtimegateway.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@ConditionalOnProperty(prefix = "badrlink.kafka", name = "enabled", havingValue = "true")
@Import(KafkaAutoConfiguration.class)
public class KafkaConsumerConfig {
}
