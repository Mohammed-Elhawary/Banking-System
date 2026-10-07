package com.banking.notification_service.service;

import java.util.Map;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import com.banking.notification_service.dto.AccountResponse;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class NotificationService {

    private final AccountServiceClient accountServiceClient;
    private final JavaMailSender mailSender;

    /*
     * CONSUME OTP GENERATED
     *
     * Triggered by:
     * transaction.otp.generated Kafka event
     *
     * PURPOSE:
     * - Email the one-time code to the account owner.
     *
     * FLOW:
     * 1. Read transactionId, accountNumber, reason, otp and amount.
     * 2. Send an alert titled "TRANSACTION VERIFICATION REQUIRED".
     *
     * NOTE:
     * - The message body is a format string with two %s placeholders
     * but no arguments are passed, so building it throws
     * MissingFormatArgumentException and no mail is ever sent.
     * - amount is cast to String while the producer publishes a
     * BigDecimal, which becomes a Double in the map and raises
     * ClassCastException first.
     * - transactionId and otp are read but never used in the body.
     *
     * @param payload Decoded transaction.otp.generated event.
     */
    @KafkaListener(topics = "transaction.otp.generated")
    public void consumeOTPGenerator(
            @Payload Map<String, Object> payload) {

        try {

            String transactionId = (String) payload.get("transactionId");
            String accountNumber = (String) payload.get("accountNumber");
            String reason = (String) payload.get("reason");
            String otp = (String) payload.get("otp");
            String amount = payload.get("amount").toString();
            sendAlert(accountNumber,
                    "TRANSACTION VERIFICATION REQUIRED",
                    String.format(
                            "Suspicious Activity detected on your account. "
                                    + "Reason: %s. "
                                    + "A Transaction of %s is pending verification. "
                                    + "Your OTP is: %s. Valid for 5 minutes. "
                                    + "If this wasn't your transaction, please contact the bank.",
                            reason,
                            amount,
                            otp));

        } catch (Exception e) {
            log.info("Error sending OTP notification : {} ", e.getMessage());
        }
    }

    /*
     * CONSUME TRANSACTION COMPLETED
     *
     * Triggered by:
     * transaction.completed Kafka event
     *
     * PURPOSE:
     * - Notify both parties that a transfer finished successfully.
     *
     * FLOW:
     * 1. Read senderAccount, receiverAccount and amount.
     * 2. Email the sender under "DEBIT TRANSACTION COMPLETED".
     * 3. Email the receiver under "CREDIT TRANSACTION COMPLETED".
     *
     * NOTE:
     * - The producer publishes senderAccountNumber and
     * receiverAccountNumber, not senderAccount and receiverAccount, so
     * both values arrive null and the mail lookup fails.
     * - amount is cast to String while the producer publishes a
     * BigDecimal, so the cast raises ClassCastException.
     * - Exceptions are logged, so the failure is silent.
     *
     * @param payload Decoded transaction.completed event.
     */
    @KafkaListener(topics = "transaction.completed")
    public void consumeTransactionCompleted(
            @Payload Map<String, Object> payload) {
        try {
            String senderAccount = (String) payload.get("senderAccountNumber");
            String receiverAccount = (String) payload.get("receiverAccountNumber");
            String amount = payload.get("amount").toString();
            // Send notification to sender account
            sendAlert(senderAccount,
                    "DEBIT TRANSACTION COMPLETED",
                    String.format("A Transaction of %s has been completed successfully. "
                            + "Receiver Account: %s", amount, receiverAccount));

            // Send notification to receiver account
            sendAlert(receiverAccount,
                    "CREDIT TRANSACTION COMPLETED",
                    String.format("A Transaction of %s has been completed successfully. "
                            + "Sender Account: %s", amount, senderAccount));
        } catch (Exception e) {
            log.info("Error sending Transaction Completed notification : {} ", e.getMessage());
        }
    }

    /*
     * CONSUME FRAUD DETECTION
     *
     * Triggered by:
     * fraud.detected Kafka event
     *
     * PURPOSE:
     * - Notify both parties that a transaction failed and was flagged.
     *
     * FLOW:
     * 1. Read senderAccount, receiverAccount, amount and reason.
     * 2. Email the sender under "DEBIT TRANSACTION FAILED".
     * 3. Email the receiver under "CREDIT TRANSACTION FAILED".
     *
     * NOTE:
     * - The producer only publishes transactionId, accountNumber and
     * reason, so senderAccount, receiverAccount and amount all arrive
     * null and both lookups fail.
     * - Exceptions are logged, so the failure is silent.
     *
     * @param payload Decoded fraud.detected event.
     */
    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetection(@Payload Map<String, Object> payload) {
        try {
            String accountNumber = (String) payload.get("accountNumber");
            String reason = (String) payload.get("reason");
            String transactionId = (String) payload.get("transactionId");

            sendAlert(accountNumber,
                    "TRANSACTION FLAGGED - ACCOUNT BLOCKED",
                    String.format(
                            "Your transaction %s was flagged as suspicious and could not be completed. "
                                    + "Your account has been temporarily blocked for security. "
                                    + "Reason: %s. "
                                    + "Please contact customer support immediately.",
                            transactionId, reason));
        } catch (Exception e) {
            log.error("Error sending fraud.detected notification: {}", e.getMessage(), e);
        }
    }

    /*
     * CONSUME REFUND PROCESSED
     *
     * Triggered by:
     * transaction.refunded Kafka event
     *
     * PURPOSE:
     * - Inform the account owner that a refund was issued.
     *
     * FLOW:
     * 1. Read accountNumber, amount and reason.
     * 2. Send an alert titled "REFUND PROCESSED".
     *
     * NOTE:
     * - The producer spells the account key "senderAccountNumber", so
     * reading "accountNumber" yields null and the mail lookup fails.
     * - amount is cast to String while the producer publishes a
     * BigDecimal, so the cast raises ClassCastException.
     * - Exceptions are logged, so the failure is silent.
     *
     * @param payload Decoded transaction.refunded event.
     */
    @KafkaListener(topics = "transaction.refunded")
    public void consumeRefundProcessed(@Payload Map<String, Object> payload) {
        try {
            String transactionId = (String) payload.get("transactionId");
            String accountNumber = (String) payload.get("senderAccountNumber");
            String amount = payload.get("amount") != null
                    ? payload.get("amount").toString()
                    : "unknown";
            String reason = (String) payload.get("reason");

            sendAlert(accountNumber,
                    "REFUND PROCESSED",
                    String.format(
                            "A refund of %s EGP for transaction %s has been processed to your account. "
                                    + "Reason: %s",
                            amount, transactionId, reason));
        } catch (Exception e) {
            log.error("Error sending Refund Processed notification: {}", e.getMessage(), e);
        }
    }

    /*
     * CONSUME PAYMENT COMPLETED
     *
     * Triggered by:
     * payment.completed Kafka event
     *
     * PURPOSE:
     * - Inform the account owner that a payment succeeded.
     *
     * FLOW:
     * 1. Read accountNumber and amount.
     * 2. Send an alert titled "PAYMENT COMPLETED".
     *
     * NOTE:
     * - Both keys match the producer contract.
     * - amount is cast to String while the producer publishes a
     * BigDecimal, so the cast raises ClassCastException and no mail is
     * sent.
     *
     * @param payload Decoded payment.completed event.
     */
    @KafkaListener(topics = "payment.completed")
    public void consumePaymentCompleted(@Payload Map<String, Object> payload) {
        try {
            String paymentId = (String) payload.get("paymentId");
            String accountNumber = (String) payload.get("accountNumber");
            String amount = payload.get("amount") != null
                    ? payload.get("amount").toString()
                    : "unknown";
            String paymobTransactionId = (String) payload.get("paymobTransactionId");

            sendAlert(accountNumber,
                    "PAYMENT SUCCESSFUL",
                    String.format(
                            "Your payment of %s EGP has been completed successfully. "
                                    + "Payment ID: %s. Transaction reference: %s.",
                            amount, paymentId, paymobTransactionId));
        } catch (Exception e) {
            log.error("Error sending Payment Completed notification: {}", e.getMessage(), e);
        }
    }

    /*
     * CONSUME PAYMENT FAILED
     *
     * Triggered by:
     * payment.failed Kafka event
     *
     * PURPOSE:
     * - Inform the account owner that a payment failed.
     *
     * FLOW:
     * 1. Read accountNumber, amount and reason.
     * 2. Send an alert titled "PAYMENT FAILED".
     *
     * NOTE:
     * - Both keys match the producer contract.
     * - amount is cast to String while the producer publishes a
     * BigDecimal, so the cast raises ClassCastException and no mail is
     * sent.
     *
     * @param payload Decoded payment.failed event.
     */
    @KafkaListener(topics = "payment.failed")
    public void consumePaymentFailed(@Payload Map<String, Object> payload) {
        try {
            String paymentId = (String) payload.get("paymentId");
            String accountNumber = (String) payload.get("accountNumber");
            String amount = payload.get("amount") != null
                    ? payload.get("amount").toString()
                    : "unknown";
            String reason = (String) payload.get("reason");

            sendAlert(accountNumber,
                    "PAYMENT FAILED",
                    String.format(
                            "Your payment of %s EGP could not be completed. "
                                    + "Payment ID: %s. Reason: %s. "
                                    + "Please try again or contact support.",
                            amount, paymentId, reason));
        } catch (Exception e) {
            log.error("Error sending Payment Failed notification: {}", e.getMessage(), e);
        }
    }

    /*
     * SEND ALERT
     *
     * PURPOSE:
     * - Deliver one alert email to the owner of an account.
     *
     * FLOW:
     * 1. Fetch the account to resolve its email address.
     * 2. Build a plain text message with the given subject and body.
     * 3. Send it through the configured SMTP mail sender.
     *
     * NOTE:
     * - A null or unknown accountNumber makes the lookup fail and the
     * exception propagates to the calling listener's catch block.
     * - The account is fetched over HTTP on every alert, with no cache.
     *
     * @param accountNumber Account whose email should be used.
     *
     * @param title Subject line of the mail.
     *
     * @param message Plain text body of the mail.
     */
    private void sendAlert(String accountNumber, String title, String message) {

        AccountResponse accountResponse = accountServiceClient.getAccount(accountNumber);

        String email = accountResponse.getEmail();
        SimpleMailMessage mailMessage = new SimpleMailMessage();
        mailMessage.setTo(email);
        mailMessage.setSubject(title);
        mailMessage.setText(message);
        mailSender.send(mailMessage);
    }
}
