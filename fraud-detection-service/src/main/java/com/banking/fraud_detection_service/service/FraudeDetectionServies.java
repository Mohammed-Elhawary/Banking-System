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

    private final KafkaTemplate<String, Object> kafkaTemplate;

    private final RedisTemplate<String, String> redisTemplate;

    @Value("${fraud.max-transaction-per-minute}")
    private int MAX_TRANSACTION_PER_MINUTE;

    @Value("${fraud.suspicious-amount-miultiplier}")
    private int SUSPICIOUS_AMOUNT_MULTIPLIER;

    @Value("${fraud.max-balance-percentage}")
    private double MAX_BALANCE_PERCENTAGE;

    /*
     * CHECK TRANSACTION FOR FRAUD
     *
     * Triggered by:
     * transaction.initiated Kafka event
     *
     * PURPOSE:
     * - Decide whether a transfer may proceed on its own or needs OTP
     * verification.
     * - Route the Saga down the matching branch by publishing to Kafka.
     *
     * FLOW:
     * 1. Read transactionId, senderAccountNumber and amount.
     * 2. Fetch the sender's current balance from account-service.
     * 3. Run the three fraud checks.
     * 4. If fraud is suspected, publish verification.required so the
     * transaction-service generates an OTP.
     * 5. Otherwise publish fraud.check.clean so the transaction-service
     * completes the transaction.
     *
     * NOTE:
     * - The balance is read AFTER transaction-service already debited
     * the sender, so it is the post-debit balance.
     * - The two branches are mutually exclusive; exactly one topic is
     * published per transaction.
     *
     * @param payload Decoded transaction.initiated event.
     */
    public void checkTransaction(Map<String, Object> payload) {

        String transactionId = (String) payload.get("transactionId");

        String accountNumber = (String) payload.get("senderAccountNumber");

        BigDecimal amount = new BigDecimal(payload.get("amount").toString());

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

            kafkaTemplate.send(VERIFICATION_REQUIRED_TOPIC, transactionId, verificationEvent);

        } else {

            Map<String, Object> transactionCleanEvent = new HashMap<>();
            transactionCleanEvent.put("transactionId", transactionId);
            transactionCleanEvent.put("isFraud", false);
            transactionCleanEvent.put("reason", resualt.getReason());

            kafkaTemplate.send(FRAUD_CHECK_CLEAN_RESULT_TOPIC, transactionId, transactionCleanEvent);

        }

    }

    /*
     * PERFORM FRAUD CHECKS
     *
     * PURPOSE:
     * - Run every rule in order and return the first violation found.
     *
     * FLOW:
     * 1. Velocity rule: more than the configured number of transfers
     * within 60 seconds.
     * 2. Amount rule: amount above the multiplier of the account's
     * running average.
     * 3. Balance rule: amount above the configured share of the
     * remaining balance.
     * 4. If none trigger, return a clean result with no reason.
     *
     * NOTE:
     * - Short-circuits on the first match, so only one reason is
     * reported even when several rules would fire.
     *
     * @param amount        Amount of the transfer under review.
     * @param accountNumber Sender account number.
     * @param senderBalance Sender balance after the debit.
     *
     * @return FraudCheckResualt with isFraud and a reason string.
     */
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

    /*
     * CHECK TRANSACTION AGAINST BALANCE
     *
     * PURPOSE:
     * - Flag a transfer that consumes most of the remaining balance.
     *
     * FLOW:
     * 1. Compute the allowed maximum as the balance times the configured
     * percentage.
     * 2. Compare the amount against that maximum.
     *
     * NOTE:
     * - The percentage comes from fraud.max-balance-percentage, so a
     * balance of 1000 with a 0.9 setting allows at most 900.
     *
     * @param senderBalance Sender balance after the debit.
     * @param amount        Amount of the transfer under review.
     *
     * @return true when the amount exceeds the allowed maximum.
     */
    private boolean isBalanceCheckFaild(BigDecimal senderBalance, BigDecimal amount) {

        BigDecimal maxAllow = senderBalance.multiply(BigDecimal.valueOf(MAX_BALANCE_PERCENTAGE));

        log.info("Balance check - amount : {} maxAllow : {} suspicious : {} ", amount, maxAllow,
                amount.compareTo(maxAllow) > 0);
        return amount.compareTo(maxAllow) > 0;

    }

    /*
     * CHECK TRANSACTION AGAINST AMOUNT HISTORY
     *
     * PURPOSE:
     * - Flag an amount that is far larger than what this account
     * normally moves.
     *
     * FLOW:
     * 1. Read the running average from Redis under "fraud:Amount:" +
     * accountNumber.
     * 2. If no average exists yet, store this amount as the seed and
     * treat the transaction as clean.
     * 3. Otherwise compute the threshold as the average times the
     * configured multiplier.
     * 4. Update the running average to the mean of the old average and
     * this amount.
     * 5. Compare the amount against the threshold.
     *
     * NOTE:
     * - The first transaction for an account can never be flagged.
     * - The Redis key has no TTL, so the average is kept forever.
     *
     * @param accountNumber Sender account number.
     * @param amount        Amount of the transfer under review.
     *
     * @return true when the amount exceeds the threshold.
     */
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

    /*
     * CHECK TRANSACTION VELOCITY
     *
     * PURPOSE:
     * - Flag an account that issues too many transfers in a short
     * window.
     *
     * FLOW:
     * 1. Increment the counter under "fraud:velocity:" + accountNumber.
     * 2. On the first increment, attach a 60 second expiry.
     * 3. Compare the counter against the configured limit.
     *
     * NOTE:
     * - The limit is a strict greater-than, so with a setting of 5 the
     * sixth transfer inside the window is the one flagged.
     *
     * @param accountNumber Sender account number.
     *
     * @return true when the counter exceeds the configured limit.
     */
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
