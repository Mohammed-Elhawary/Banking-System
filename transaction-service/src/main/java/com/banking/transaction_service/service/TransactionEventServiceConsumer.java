package com.banking.transaction_service.service;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import com.banking.transaction_service.entity.Transaction;
import com.banking.transaction_service.entity.TransactionStatus;
import com.banking.transaction_service.repository.TransactionRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/*
 * SAGA EVENT CONSUMER
 *
 * This service listens to Kafka events related to
 * transaction verification and fraud detection.
 *
 * Responsibilities:
 * - Generate OTP when verification is required.
 * - Store OTP temporarily in Redis.
 * - Change transaction status to PENDING_VERIFICATION.
 * - Publish OTP event for Notification Service.
 * - Continue the Saga when Fraud Detection confirms
 * that the transaction is clean.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionEventServiceConsumer {

    private final TransactionRepository transactionRepository;
    private final TransactionService transactionService;

    private final int OTP_EXPIRY_MINUTE = 5;
    private final RedisTemplate<String, Object> redis;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    SecureRandom random = new SecureRandom();

    private final static String TRANSACTION_OTP_GENERATED_TOPIC = "transaction.otp.generated";

    /*
     * SAGA STEP - Generate OTP
     *
     * Triggered by:
     * verification.required Kafka event
     *
     * @param payload
     *
     * FLOW:
     * 1. Get transactionId, accountNumber and reason from event.
     * 2. Find transaction in database.
     * 3. Check that transaction is still PROCESSING.
     * 4. Generate a 6-digit OTP.
     * 5. Store OTP in Redis with 5-minute expiration.
     * 6. Change transaction status to PENDING_VERIFICATION.
     * 7. Publish transaction.otp.generated event.
     *
     * RESULT:
     * - OTP is stored temporarily in Redis.
     * - User can receive the OTP through Notification Service.
     * - Transaction waits for OTP verification.
     */
    @KafkaListener(topics = "verification.required")
    public void consumeVerificationRequired(@Payload Map<String, Object> payload) {

        try {

            String transactionId = (String) payload.get("transactionId");
            String accountNumber = (String) payload.get("accountNumber");
            String reason = (String) payload.get("reason");

            log.info("Verification required -Transaction {} Reason {} ", transactionId, reason);

            Transaction transaction = transactionRepository.findById(transactionId).orElseThrow(

                    () -> new RuntimeException("Transaction not found " + transactionId));

            if (transaction.getTransactionStatus() != TransactionStatus.PROCCESSING) {

                log.warn("Transaction {} not PROCCESSING - skipping ", transactionId);
                return;
            }

            // Generate 6 digit otp
            String otp = String.format(
                    "%06d",
                    random.nextInt(900000) + 100000);
            // Store OTP in Redis - expire in 5 minutes

            String otpKey = "verification:otp:" + transactionId;

            redis.opsForValue().set(otpKey, otp, Duration.ofMinutes(OTP_EXPIRY_MINUTE));

            transaction.setTransactionStatus(TransactionStatus.PENDING_VERIFICATION);
            transactionRepository.save(transaction);

            log.info("OTP generated for transaction : {} expire in : {}", transactionId, redis.getExpire(otpKey));

            // Notify user
            Map<String, Object> otpEvent = new HashMap<>();

            otpEvent.put("transactionId", transactionId);
            otpEvent.put("accountNumber", accountNumber);
            otpEvent.put("otp", otp);
            otpEvent.put("amount", payload.get("amount"));
            otpEvent.put("reason", reason);

            kafkaTemplate.send(TRANSACTION_OTP_GENERATED_TOPIC, transactionId, otpEvent);

        } catch (Exception e) {

            log.error("Error handling verification requird {} ", e.getMessage());

        }
    }

    /*
     * SAGA CONTINUATION - Fraud Check Clean Result
     *
     * Triggered by:
     * fraud.check.clean Kafka event
     *
     * @param payload
     *
     * FLOW:
     * 1. Get transactionId from Kafka event.
     * 2. Send transactionId to TransactionService.
     * 3. Verify that transaction is still PROCESSING.
     * 4. Continue the Saga and complete the transaction.
     *
     * RESULT:
     * - Fraud Detection confirmed that the transaction is clean.
     * - Transaction continues to the next Saga step.
     */
    @KafkaListener(topics = "fraud.check.clean")
    public void consumeFraudCheckCleanResult(@Payload Map<String, Object> payload) {

        try {

            String transactionId = (String) payload.get("transactionId");
            transactionService.processCleanResult(transactionId);

        } catch (Exception e) {
            log.error("Error processing fruad check result : {} ", e.getMessage());
        }

    }
}
