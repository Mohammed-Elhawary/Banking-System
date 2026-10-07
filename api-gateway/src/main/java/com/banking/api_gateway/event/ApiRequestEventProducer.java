package com.banking.api_gateway.event;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.banking.api_gateway.dto.ApiRequestEvent;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class ApiRequestEventProducer {
    
    private final String ANALYSIS_TOPIC = "api-request-events";
    private final KafkaTemplate<String, ApiRequestEvent> kafkaTemplate;

    public void sendToAnalysisService(ApiRequestEvent event) {
    
        kafkaTemplate.send(ANALYSIS_TOPIC,event.getRequestId(), event);
    }
}
