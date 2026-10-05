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
                            otp
                    ));

        } catch (Exception e) {
            log.info("Error sending OTP notification : {} ", e.getMessage());
        }
    }

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

    @KafkaListener(topics = "fraud.detected")
    public void consumeFraudDetection(
            @Payload Map<String, Object> payload) {
        try {
            String senderAccount = (String) payload.get("senderAccount");
            String receiverAccount = (String) payload.get("receiverAccount");
            String amount = (String) payload.get("amount");
            String reason = (String) payload.get("reason");

            // Send notification to sender account
            sendAlert(senderAccount,
                    "DEBIT TRANSACTION FAILED",
                    String.format("A Transaction of %s has failed. and your account has been blocked. "
                            + "Receiver Account: %s. Reason: %s"
                            + " please contact customer  bank support immediately.", amount, receiverAccount, reason));

            // Send notification to receiver account
            sendAlert(receiverAccount,
                    "CREDIT TRANSACTION FAILED",
                    String.format(
                            "A Transaction of %s has failed. and your account has been flagged for suspicious activity. "
                            + "Sender Account: %s. Reason: %s"
                            + " please contact customer  bank support immediately.",
                            amount, senderAccount, reason));
        } catch (Exception e) {
            log.info("Error sending Transaction Failed notification : {} ", e.getMessage());
        }
    }

    @KafkaListener(topics = "transaction.refunded")
    public void consumeRefundProcessed(@Payload Map<String, Object> payload) {
        try {
            String accountNumber = (String) payload.get("accountNumber");
            String amount = (String) payload.get("amount");
            String reason = (String) payload.get("reason");

            sendAlert(accountNumber,
                    "REFUND PROCESSED",
                    String.format("A refund of %s has been processed to your account. "
                            + "Reason: %s", amount, reason));
        } catch (Exception e) {
            log.info("Error sending Refund Processed notification : {} ", e.getMessage());
        }
    }

    @KafkaListener(topics = "payment.completed")
    public void consumePaymentCompleted(@Payload Map<String, Object> payload) {
        try {
            String accountNumber = (String) payload.get("accountNumber");
            String amount = (String) payload.get("amount");

            sendAlert(accountNumber,
                    "PAYMENT COMPLETED",
                    String.format("A payment of %s has been completed successfully.", amount));
        } catch (Exception e) {
            log.info("Error sending Payment Completed notification : {} ", e.getMessage());
        }
    }

    @KafkaListener(topics = "payment.failed")
    public void consumePaymentFailed(@Payload Map<String, Object> payload) {
        try {
            String accountNumber = (String) payload.get("accountNumber");
            String amount = (String) payload.get("amount");
            String reason = (String) payload.get("reason");

            sendAlert(accountNumber,
                    "PAYMENT FAILED",
                    String.format("A payment of %s has failed. Reason: %s", amount, reason));
        } catch (Exception e) {
            log.info("Error sending Payment Failed notification : {} ", e.getMessage());
        }
    }

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
