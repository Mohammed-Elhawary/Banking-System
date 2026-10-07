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
     * CONSUME TRANSACTION COMPLETED - CREDIT RECEIVER
     *
     * Triggered by:
     * transaction.completed Kafka event
     *
     * PURPOSE:
     * - Deliver the transferred amount to the receiver account.
     *
     * FLOW:
     * 1. Read receiverAccountNumber and amount from the event.
     * 2. Call the service layer to credit that account.
     *
     * NOTE:
     * - This is the only path that credits the receiver. The credit
     * call inside transaction-service's completeTransactionResponse
     * is commented out, so the event is what actually moves the money.
     * - By the time this runs, the transaction is already marked
     * COMPLETED. If the credit throws, the exception is logged and
     * swallowed with no retry, and the amount is lost for good.
     * - No status guard, so a redelivered event credits twice.
     *
     * @param payload Decoded transaction.completed event.
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
     * CONSUME FRAUD DETECTION - BLOCK ACCOUNT
     *
     * Triggered by:
     * fraud.detected Kafka event
     *
     * PURPOSE:
     * - Freeze the sender account after a failed OTP check.
     *
     * FLOW:
     * 1. Read accountNumber from the event.
     * 2. Call the service layer to block that account.
     *
     * NOTE:
     * - The event is consumed asynchronously, while transaction-service
     * performs the refund synchronously right after publishing it. If
     * this block lands first, the refund credit is rejected because the
     * account is no longer ACTIVE.
     * - Exceptions are logged and swallowed, so the account may stay
     * unblocked.
     *
     * @param payload Decoded fraud.detected event.
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
