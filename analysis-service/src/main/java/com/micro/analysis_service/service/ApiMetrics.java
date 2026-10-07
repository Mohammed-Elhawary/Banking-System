package com.micro.analysis_service.service;

import org.springframework.stereotype.Component;

import com.micro.analysis_service.dto.ApiRequestEvent;

import io.micrometer.core.instrument.MeterRegistry;

@Component
public class ApiMetrics {

   private final MeterRegistry registry;

    public ApiMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

public void recordRequest(ApiRequestEvent event) {

    registry.counter(
            "api_requests_total",
            "service", event.getService(),
            "status", String.valueOf(event.getStatus())
    ).increment();

    registry.summary(
            "api_request_duration_ms",
            "service", event.getService()
    ).record(event.getDurationMs());
}
}
