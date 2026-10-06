package com.banking.api_gateway.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.banking.api_gateway.dto.ApiRequestEvent;
import com.banking.api_gateway.event.ApiRequestEventProducer;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/test/kafka")
@RequiredArgsConstructor
public class KafkaTestController {

    private final ApiRequestEventProducer producer;

    @PostMapping
    public ApiRequestEvent sendTestEvent() {

        ApiRequestEvent event = ApiRequestEvent.builder()
                .requestId("test-123")
                .method("POST")
                .path("/test/kafka")
                .service("api-gateway")
                .status(200)
                .durationMs(150)
                .timestamp("2026-10-06T13:30:00Z")
                .build();

        producer.sendToAnalysisService(event);

        return event;
    }

    
}
