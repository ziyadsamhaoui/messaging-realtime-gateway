package com.ziyadsamhaoui.messagingrealtimegateway.observability;

import com.ziyadsamhaoui.messagingrealtimegateway.stomp.StompSessionRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.security.Principal;

import static org.assertj.core.api.Assertions.assertThat;

class StompConnectionMetricsTest {

    private final StompSessionRegistry sessionRegistry = new StompSessionRegistry();
    private SimpleMeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        new StompConnectionMetrics(meterRegistry, sessionRegistry);
    }

    private static Principal principal(String name) {
        return () -> name;
    }

    @Test
    void gaugeStartsAtZero() {
        assertThat(gauge().value()).isZero();
    }

    @Test
    void gaugeFollowsRegisteredAndDisconnectedSessions() {
        sessionRegistry.register("s1", principal("alice"));
        sessionRegistry.register("s2", principal("bob"));
        assertThat(gauge().value()).isEqualTo(2.0);

        sessionRegistry.unregister("s1");
        assertThat(gauge().value()).isEqualTo(1.0);

        sessionRegistry.unregister("s2");
        assertThat(gauge().value()).isZero();
    }

    private io.micrometer.core.instrument.Gauge gauge() {
        return meterRegistry.get(StompConnectionMetrics.ACTIVE_CONNECTIONS_METRIC).gauge();
    }
}
