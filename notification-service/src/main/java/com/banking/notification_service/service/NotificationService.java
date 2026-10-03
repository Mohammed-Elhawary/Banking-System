package com.banking.notification_service.service;

import java.util.Map;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class NotificationService {

    @KafkaListener(topics = "transaction.otp.generated")
    public void consumeOTPGenerator(
            @Payload Map<String, Object> payload) {

        try {

            String transactionId = (String) payload.get("transactionId");
            String accountNumber = (String) payload.get("accountNumber");
            String reason = (String) payload.get("reason");
            String otp = (String) payload.get("otp");
            String amount = (String) payload.get("amount");

            sendAlert(accountNumber,
                    "TRANSACTION VERIFICATION REQUIRED",
                    String.format("Suspious Activity detected on your account " +
                            "Reason %s " +
                            "A Transaction of %s is pending verification.  " +
                            "Your OTP is: %s Valid for 5 minutes. " +
                            "If this wasn't your - ignore this message"));

        } catch (Exception e) {
            log.info("Error sending OTP notification : {} ", e.getMessage());
        }
    }

    private void sendAlert(String accountNumber, String string, String format) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'sendAlert'");
    }
}
