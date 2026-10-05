package com.banking.payment_service.service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import com.banking.payment_service.config.PaymobProperties;
import com.banking.payment_service.dto.CreatePaymentRequest;
import com.banking.payment_service.dto.PaymentOrderResponse;
import com.banking.payment_service.dto.PaymobIntentionResponse;
import com.banking.payment_service.entity.Payment;
import com.banking.payment_service.entity.PaymentStatus;
import com.banking.payment_service.repository.PaymentRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final PaymobProperties paymobProperties;
    private final PaymobHmacService paymobHmacService;
    private final RestClient paymobRestClient;

    private final String PAYMENT_COMPLETED_TOPIC = "payment.completed";
    private final String PAYMENT_FAILED_TOPIC = "payment.failed";

    /*
         * CREATE PAYMOB PAYMENT INTENTION
         *
         * PURPOSE:
         * - Create a new payment intention in Paymob.
         * - Save the payment information in our database.
         * - Return the payment details to the frontend.
         *
         * FLOW:
         * 1. Receive payment details from the frontend.
         * 2. Create a payment intention in Paymob.
         * 3. Save the payment record in our database.
         * 4. Return the Paymob payment details to the frontend.
         * 5. Frontend opens Paymob Unified Checkout.
         * 6. User completes the payment.
         *
         * AFTER PAYMENT:
         * - Paymob sends the payment result to our Webhook.
         * - Webhook processes the payment status.
         *
         * @param request Payment details received from the frontend.
         *
         * @return PaymentOrderResponse containing the Paymob payment details.
     */
    public PaymentOrderResponse createPaymentOrder(CreatePaymentRequest request) {

        log.info(
                "Creating payment intention for account: {} amount: {}",
                request.getAccountNumber(),
                request.getAmount());

        /*
                 * Convert the amount to the smallest currency unit.
                 * Example: 1 EGP -> 100 Qirsh
         */
        int convertedAmount = request.getAmount()
                .multiply(BigDecimal.valueOf(100))
                .intValue();

        Map<String, Object> body = new HashMap<>();

        body.put("amount", convertedAmount);
        body.put("currency", "EGP");
        body.put(
                "payment_methods",
                List.of(Integer.parseInt(paymobProperties.integrationId())));

        body.put(
                "special_reference",
                "payment_" + UUID.randomUUID());
        body.put(
                "notification_url",
                "https://underarm-daylong-tradition.ngrok-free.dev/api/v1/payments/webhook"
        );

        body.put(
                "redirection_url",
                "http://localhost:8084/api/v1/payments/payment-result"
        );
        Map<String, Object> billingData = new HashMap<>();

        billingData.put("first_name", "Mohamed");
        billingData.put("last_name", "Saad");
        billingData.put("email", "fraud.test@bank.test");
        billingData.put("phone_number", "01000000000");
        billingData.put("country", "EG");
        billingData.put("city", "Alexandria");
        billingData.put("street", "Test Street");
        billingData.put("building", "1");

        body.put("billing_data", billingData);
        PaymobIntentionResponse paymobResponse = paymobRestClient.post()
                .uri("/v1/intention/")
                .header(
                        HttpHeaders.AUTHORIZATION,
                        "Token " + paymobProperties.secretKey())
                .contentType(MediaType.APPLICATION_JSON)
                .body(body)
                .retrieve()
                .body(PaymobIntentionResponse.class);

        log.info(
                "Paymob payment intention created: {}",
                paymobResponse.getId());

        Payment payment = Payment.builder()
                .accountNumber(request.getAccountNumber())
                .paymentStatus(PaymentStatus.CREATED)
                .amount(request.getAmount())
                .currency("EGP")
                .paymobIntentionId(paymobResponse.getId())
                .paymobOrderId(paymobResponse.getIntentionOrderId().toString())
                .clientSecret(paymobResponse.getClientSecret())
                .description(request.getDescription())
                .build();

        Payment savedPayment = paymentRepository.save(payment);
        return PaymentOrderResponse.builder()
                .paymentId(savedPayment.getId())
                .paymobIntentionId(savedPayment.getPaymobIntentionId())
                .paymobOrderId(savedPayment.getPaymobOrderId().toString())
                .amount(savedPayment.getAmount())
                .publicKey(paymobProperties.publicKey())
                .clientSecret(savedPayment.getClientSecret())
                .currency(savedPayment.getCurrency())
                .paymentStatus(savedPayment.getPaymentStatus().toString())
                .build();
    }

    public void handleWebhook(Map<String, Object> payload
        ,String receivedHmac
    ) {

        log.info("Received Paymob webhook");

        try {

            Map<String, Object> paymentData
                    = extractPaymentData(payload);
            log.info(
                    "Paymob webhook HMAC present: {}",
                    payload.containsKey("hmac")
            );

            String calculatedHmac
                    = paymobHmacService.calculateHmac(payload);

            if (!calculatedHmac.equalsIgnoreCase(receivedHmac)) {

                log.warn("Invalid Paymob webhook HMAC");

                throw new SecurityException(
                        "Invalid Paymob webhook HMAC"
                );
            }

            log.info("Paymob webhook HMAC verified successfully");

            Boolean success
                    = (Boolean) paymentData.get("success");

            if (Boolean.TRUE.equals(success)) {
                handlePaymentSuccess(paymentData);
            } else {
                handlePaymentFailure(paymentData);
            }

        } catch (Exception e) {

            log.error(
                    "Error handling Paymob webhook: {}",
                    e.getMessage(),
                    e
            );

            throw e;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> extractPaymentData(
            Map<String, Object> payload) {

        Map<String, Object> paymentData
                = (Map<String, Object>) payload.get("obj");

        if (paymentData == null) {
            throw new IllegalArgumentException(
                    "Missing 'obj' in Paymob webhook payload");
        }

        return paymentData;
    }

    @SuppressWarnings("unchecked")
    private void handlePaymentSuccess(
            Map<String, Object> paymentData) {

        Map<String, Object> order
                = (Map<String, Object>) paymentData.get("order");

        if (order == null || order.get("id") == null) {
            throw new IllegalArgumentException(
                    "Missing order information in Paymob webhook");
        }

        Number orderIdNumber
                = (Number) order.get("id");

        String orderId
                = orderIdNumber.toString();

        String transactionId
                = String.valueOf(paymentData.get("id"));

        Payment payment
                = paymentRepository
                        .findByPaymobOrderId(orderId)
                        .orElseThrow(
                                () -> new RuntimeException(
                                        "Payment not found for order-id "
                                        + orderId));

        payment.setPaymentStatus(
                PaymentStatus.COMPLETED);

        payment.setPaymobTransactionId(
                transactionId);

        paymentRepository.save(payment);

        Map<String, Object> message = Map.of(
                "paymentId", payment.getId(),
                "accountNumber", payment.getAccountNumber(),
                "amount", payment.getAmount(),
                "paymobTransactionId", transactionId);
        log.info(
                "Payment completed: paymentId={}, orderId={}, transactionId={}",
                payment.getId(),
                orderId,
                transactionId
        );
        kafkaTemplate.send(
                PAYMENT_COMPLETED_TOPIC,
                payment.getId(),
                message);
    }

    /*
         * HANDLE PAYMENT FAILURE
         *
         * PURPOSE:
         * - Process a failed payment notification received from Paymob.
         * - Find the related payment in our database.
         * - Update the payment status to FAILED.
         * - Publish a payment-failed event to Kafka.
         *
         * FLOW:
         * 1. Receive the Paymob webhook payload.
         * 2. Extract the payment/transaction information.
         * 3. Find our local Payment record.
         * 4. Mark the payment as FAILED.
         * 5. Save the updated payment.
         * 6. Publish PAYMENT_FAILED event to Kafka.
         *
         * @param payload Payment failure data received from Paymob.
     */
    @SuppressWarnings("unchecked")
    private void handlePaymentFailure(
            Map<String, Object> paymentData) {

        try {
            Map<String, Object> order = (Map<String, Object>) paymentData.get("order");

            if (order == null || order.get("id") == null) {
                throw new IllegalArgumentException(
                        "Missing order information in Paymob webhook");
            }

            Number orderIdNumber = (Number) order.get("id");

            String orderId = orderIdNumber.toString();

            Payment payment = paymentRepository
                    .findByPaymobOrderId(orderId)
                    .orElseThrow(
                            () -> new RuntimeException(
                                    "Payment not found for order-id "
                                    + orderId));

            payment.setPaymentStatus(
                    PaymentStatus.FAILED);

            payment.setFailureReason(
                    "Payment failed via Paymob webhook");

            paymentRepository.save(payment);

            Map<String, Object> message = Map.of(
                    "paymentId", payment.getId(),
                    "accountNumber", payment.getAccountNumber(),
                    "amount", payment.getAmount(),
                    "reason", payment.getFailureReason());

            kafkaTemplate.send(
                    PAYMENT_FAILED_TOPIC,
                    payment.getId(),
                    message);

            log.info(
                    "Payment failed for paymentId: {} and orderId: {}",
                    payment.getId(),
                    orderId);

        } catch (Exception e) {
            log.error(
                    "Error handling payment failure webhook: {}",
                    e.getMessage(),
                    e);
        }
    }

    public Payment getPaymentByTransactionId(String transactionId) {

        return paymentRepository
                .findByPaymobTransactionId(transactionId)
                .orElseThrow(()
                        -> new RuntimeException(
                        "Payment not found for transaction: "
                        + transactionId));
    }

}
