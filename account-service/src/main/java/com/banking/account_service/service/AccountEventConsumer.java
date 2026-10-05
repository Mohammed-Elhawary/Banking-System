package com.banking.account_service.service;

import java.math.BigDecimal;
import java.util.Map;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * AccountEventConsumer
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class AccountEventConsumer {

    private final AccountService accountService;

    /*
     * consume transaction complete event kafka
     * Credit reciver account
     *
     * @param payload
     */

    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(@Payload Map<String, Object> payload) {

        try {

            String receiverAccount = (String) payload.get("receiverAccountNumber");

            BigDecimal amount = new BigDecimal(payload.get("amount").toString());
            log.info("Crediting Blance : {} amount : {} ", receiverAccount, amount);

            accountService.creditBalance(receiverAccount, amount);

        } catch (Exception e) {
            log.error("Error Crediting account : {} ", e.getMessage());
        }

    }

    /*
     * consume Fraud detection event from kafka
     *
     * Block the flagg account
     *
     * @param payload
     *
     */
    @KafkaListener(topics = "fraud.detected")

    public void consumeFraudDetection(@Payload Map<String, Object> payload) {

        try {
            String accountNumber = (String) payload.get("accountNumber");
            log.info("Fraud detection - blocking account {}", accountNumber);
            accountService.blockAccount(accountNumber);
        } catch (Exception e) {
            log.error("Error Blocking account : {} ", e.getMessage());
        }
    }
}
