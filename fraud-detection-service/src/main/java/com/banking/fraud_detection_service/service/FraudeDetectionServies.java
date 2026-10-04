package com.banking.fraud_detection_service.service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.banking.fraud_detection_service.client.AccountServiceClient;
import com.banking.fraud_detection_service.model.FraudCheckResualt;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class FraudeDetectionServies {

    private final AccountServiceClient accountServiceClient;

    private final String VERIFICATION_REQUIRED_TOPIC = "verification.required";
    private final String FRAUD_CHECK_CLEAN_RESULT_TOPIC = "fraud.check.clean";

    private final KafkaTemplate<String, Object> KafkaTemplate;;

    private final RedisTemplate<String, String> redisTemplate;

    @Value("${fraud.max-transaction-per-minute}")
    private int MAX_TRANSACTION_PER_MINUTE;

    @Value("${fraud.suspicious-amount-miultiplier}")
    private int SUSPICIOUS_AMOUNT_MULTIPLIER;

    @Value("${fraud.max-balance-percentage}")
    private double MAX_BALANCE_PERCENTAGE;

    public void checkTransaction(Map<String, Object> payload) {

        String transactionId = (String) payload.get("transactionId");

        String accountNumber = (String) payload.get("senderAccountNumber");

        BigDecimal amount = new BigDecimal(payload.get("receiverAccountNumber").toString());

        BigDecimal senderBalance = accountServiceClient.GetBalance(accountNumber);

        log.info("Checking transaction : {} account : {} amount: {} balance : {} ", transactionId, accountNumber,
                amount, senderBalance);

        FraudCheckResualt resualt = performFraudChecks(amount, accountNumber, senderBalance);

        if (resualt.isFraud()) {

            log.info("Suspicious activity detected - account: {} " + "reason : {} - requisting OTP Verification ",
                    accountNumber, resualt.getReason());

            Map<String, Object> verificationEvent = new HashMap<>();
            verificationEvent.put("accountNumber", accountNumber);
            verificationEvent.put("transactionId", transactionId);
            verificationEvent.put("amount", amount);
            verificationEvent.put("reason", resualt.getReason());

            KafkaTemplate.send(VERIFICATION_REQUIRED_TOPIC, transactionId, verificationEvent);

        } else {

            Map<String, Object> transactionCleanEvent = new HashMap<>();
            transactionCleanEvent.put("transactionId", transactionId);
            transactionCleanEvent.put("isFraud", false);
            transactionCleanEvent.put("reason", resualt.getReason());

            KafkaTemplate.send(FRAUD_CHECK_CLEAN_RESULT_TOPIC, transactionId, transactionCleanEvent);

        }

    }

    private FraudCheckResualt performFraudChecks(BigDecimal amount, String accountNumber, BigDecimal senderBalance) {

        if (isVelocityExceeded(accountNumber)) {
            return FraudCheckResualt.builder()
                    .fraud(true)
                    .reason("Too many transaction in 60 secound - velocity limit excedded")
                    .build();
        }
        if (isAmountSuspicious(accountNumber, amount)) {
            return FraudCheckResualt.builder()
                    .fraud(true)
                    .reason("Unusual transaction amount  - exceeds 3x your avarege ")
                    .build();
        }

        if (senderBalance.compareTo(BigDecimal.ZERO) > 0 && isBalanceCheckFaild(senderBalance, amount)) {

            return FraudCheckResualt.builder().fraud(true).reason("Transaction exceed 90% of account balance").build();
        }

        return FraudCheckResualt.builder().fraud(false).reason(null).build();
    }

    private boolean isBalanceCheckFaild(BigDecimal senderBalance, BigDecimal amount) {

        BigDecimal maxAllow = senderBalance.multiply(BigDecimal.valueOf(MAX_BALANCE_PERCENTAGE));

        log.info("Balance check - amount : {} maxAllow : {} suspicious : {} ", amount, maxAllow,
                maxAllow.compareTo(amount) > 0);
        return maxAllow.compareTo(amount) > 0;

    }

    private boolean isAmountSuspicious(String accountNumber, BigDecimal amount) {

        String avgKey = "fraud:Amount:" + accountNumber;
        String avrStr = redisTemplate.opsForValue().get(avgKey);

        if (avrStr == null) {
            redisTemplate.opsForValue().set(avgKey, amount.toString());
            return false;
        }
        BigDecimal avgAmount = new BigDecimal(redisTemplate.opsForValue().get(avgKey));

        BigDecimal threshold = avgAmount.multiply(BigDecimal.valueOf(SUSPICIOUS_AMOUNT_MULTIPLIER));

        BigDecimal newAvg = avgAmount.add(amount).divide(BigDecimal.valueOf(2), 2, RoundingMode.HALF_UP);

        redisTemplate.opsForValue().set(avgKey, newAvg.toString());

        boolean suspicious = amount.compareTo(threshold) > 0;

        log.info("Amount check - amount : {} thershold : {} suspicious : {} ", amount, threshold,
                suspicious);

        return suspicious;

    }

    private boolean isVelocityExceeded(String accountNumber) {

        String key = "fraud:velocity:" + accountNumber;
        Long count = redisTemplate.opsForValue().increment(key);
        if (count != null && count == 1) {
            redisTemplate.expire(key, Duration.ofSeconds(60));

        }

        log.info("Velocity check - account {} count {}", accountNumber, count);
        return count != null && count > MAX_TRANSACTION_PER_MINUTE;

    }

}
