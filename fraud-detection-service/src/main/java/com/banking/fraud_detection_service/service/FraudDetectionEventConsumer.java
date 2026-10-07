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

    /*
     * CONSUME TRANSACTION INITIATED
     *
     * Triggered by:
     * transaction.initiated Kafka event
     *
     * PURPOSE:
     * - Start the fraud check for every new transfer.
     *
     * FLOW:
     * 1. Log that a transaction arrived for review.
     * 2. Hand the payload to the fraud check service.
     *
     * NOTE:
     * - Exceptions are caught and logged, so a failed check publishes
     * neither verification.required nor fraud.check.clean. The
     * transaction then stays in PROCCESSING with no way to move.
     *
     * @param payload Decoded transaction.initiated event.
     */
    @KafkaListener(topics = "transaction.initiated")
    public void consumeTransactionInitiate(@Payload Map<String, Object> payload) {

        log.info("Received  transaction for fraud cheak : {}", payload.get("transactionId"));
        try {
            fraudeDetectionServies.checkTransaction(payload);
        } catch (Exception e) {
            log.error("Fraud check failed for tx {}: {}", payload.get("transactionId"), e.getMessage());
        }
    }
}
