package com.ziyadsamhaoui.messagingrealtimegateway.observability;

import com.ziyadsamhaoui.messagingrealtimegateway.stomp.StompSessionRegistry;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class StompConnectionMetrics {

    static final String ACTIVE_CONNECTIONS_METRIC = "badrlink.realtime.stomp.connections.active";

    public StompConnectionMetrics(MeterRegistry meterRegistry, StompSessionRegistry sessionRegistry) {
        Gauge.builder(ACTIVE_CONNECTIONS_METRIC, sessionRegistry, StompSessionRegistry::localSessionCount)
                .description("Currently registered local STOMP sessions")
                .register(meterRegistry);
    }
}
