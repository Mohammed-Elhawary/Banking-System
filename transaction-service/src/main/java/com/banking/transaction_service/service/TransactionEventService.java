package com.banking.transaction_service.service;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;

import com.banking.transaction_service.entity.Transaction;
import com.banking.transaction_service.entity.TransactionStatus;
import com.banking.transaction_service.repository.TransactionRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class TransactionEventService {

    private final TransactionService transactionService;
    private final TransactionRepository transactionrRepository;
    private final int OTP_EXPIRY_MINUTE = 5;
    private final RedisTemplate<String, String> redis;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private final static String TRANSACTION_OTP_GENERATED_TOPIC = "transaction.otp.generated";

    @KafkaListener(topics = "verification.required")
    public void verification(@Payload Map<String, Object> payload) {

        try {

            String transactionId = (String) payload.get("transactionId");
            String accountNumber = (String) payload.get("accountNumber");
            String reason = (String) payload.get("reason");

            log.info("Verification required -Transaction {} Reason {} ", transactionId, reason);

            Transaction transaction = transactionrRepository.findById(accountNumber).orElseThrow(

                    () -> new RuntimeException("Transaction not found " + transactionId));

            if (transaction.getTransactionStatus() != TransactionStatus.PROCCESSING) {

                log.info("Transaction {} not PROCCESSING - skipping ", transactionId);
                return;
            }

            // Generate 6 digit otp
            String otp = String.format("%06D", (int) (Math.random() + 900000) + 100000);

            // Store OTP in Redis - expire in 5 minutes

            String otpKey = "verification:otp" + otp;

            redis.opsForValue().set(otpKey, otp, Duration.ofMinutes(OTP_EXPIRY_MINUTE));

            transaction.setTransactionStatus(TransactionStatus.PENDING_VERIFICATION);
            transactionrRepository.save(transaction);

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

}
