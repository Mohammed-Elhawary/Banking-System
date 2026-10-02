package com.banking.fraud_detection_service.service;

import java.util.Map;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class FraudDetectionEventConsumer {

    private final FraudeDetectionServies fraudeDetectionServies;

    @KafkaListener(topics = "transaction.initiated")
    public void consumeTransactionInitiate(@Payload Map<String, Object> payload) {

        log.info("Received  transaction for fraud cheak : {}", payload.get("transactionId"));
        try {
            fraudeDetectionServies.checkTransaction(payload);
        } catch (Exception e) {
            // TODO: handle exception
        }
    }
}
