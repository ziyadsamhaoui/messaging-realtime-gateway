package com.ziyadsamhaoui.messagingrealtimegateway.observability;

import com.ziyadsamhaoui.messagingrealtimegateway.stomp.StompSessionRegistry;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * Number of STOMP sessions this instance currently holds.
 *
 * <p>The value is read from the registry at scrape time instead of being counted on connect and
 * disconnect: the registry is already the single source of truth, and a counter updated in two
 * places would drift whenever a disconnect is missed.
 */
@Component
public class StompConnectionMetrics {

    static final String ACTIVE_CONNECTIONS_METRIC = "badrlink.realtime.stomp.connections.active";

    public StompConnectionMetrics(MeterRegistry meterRegistry, StompSessionRegistry sessionRegistry) {
        Gauge.builder(ACTIVE_CONNECTIONS_METRIC, sessionRegistry, StompSessionRegistry::localSessionCount)
                .description("Currently registered local STOMP sessions")
                .register(meterRegistry);
    }
}
