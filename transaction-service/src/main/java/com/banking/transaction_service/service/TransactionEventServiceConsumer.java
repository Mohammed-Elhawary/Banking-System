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
     * SAGA STEP - GENERATE OTP
     *
     * Triggered by:
     * verification.required Kafka event
     *
     * PURPOSE:
     * - Issue an OTP for a transaction the fraud check flagged.
     * - Park the transaction until the user submits that OTP.
     *
     * FLOW:
     * 1. Read transactionId, accountNumber and reason from the event.
     * 2. Load the transaction from the database.
     * 3. Return early unless the status is PROCCESSING.
     * 4. Generate a random 6-digit code.
     * 5. Store it in Redis under "verification:otp:" + transactionId
     * with a 5 minute TTL.
     * 6. Set the status to PENDING_VERIFICATION and save.
     * 7. Publish transaction.otp.generated so notification-service can
     * deliver the code.
     *
     * RESULT:
     * - The OTP lives in Redis until it expires or is consumed.
     * - The transaction waits in PENDING_VERIFICATION.
     * - No money moves until verifyOTP accepts the code.
     *
     * NOTE:
     * - Exceptions are caught and logged, so a failure here is silent
     * and the transaction stays in PROCCESSING forever.
     * - The amount is forwarded as the raw object taken from the
     * incoming payload, not re-read from the entity.
     *
     * @param payload Decoded verification.required event.
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
     * SAGA CONTINUATION - FRAUD CHECK CLEAN RESULT
     *
     * Triggered by:
     * fraud.check.clean Kafka event
     *
     * PURPOSE:
     * - Continue the Saga for a transaction the fraud check cleared.
     *
     * FLOW:
     * 1. Read the transactionId from the event.
     * 2. Hand it to the service layer, which completes the transaction
     * if it is still PROCCESSING.
     *
     * RESULT:
     * - The transaction becomes COMPLETED and the receiver is credited
     * through transaction.completed.
     * - No OTP is involved: a clean result completes the transaction
     * directly.
     *
     * NOTE:
     * - This listener ignores the isFraud and reason fields of the
     * event and trusts the topic alone.
     * - Exceptions are caught and logged, so a failure is silent and
     * the transaction stays in PROCESSING.
     *
     * @param payload Decoded fraud.check.clean event.
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
