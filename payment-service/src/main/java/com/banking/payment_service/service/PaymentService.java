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

    /*
         * HANDLE PAYMOB WEBHOOK
         *
         * Triggered by:
         * POST /api/v1/payments/webhook
         *
         * PURPOSE:
         * - Verify an incoming Paymob callback and route it to the
         * success or failure path.
         *
         * FLOW:
         * 1. Extract the payment object from the "obj" key.
         * 2. Recompute the HMAC over the payload.
         * 3. Reject the call with a SecurityException when the computed
         * value does not match the received one.
         * 4. Dispatch to handlePaymentSuccess or handlePaymentFailure
         * based on the "success" flag.
         *
         * NOTE:
         * - Exceptions are logged and rethrown, so Paymob can retry the
         * callback.
         * - The comparison ignores case.
         *
         * @param payload    Raw webhook body as sent by Paymob.
         * @param receivedHmac The HMAC Paymob sent in the query string.
         */
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

    /*
         * EXTRACT PAYMENT DATA
         *
         * PURPOSE:
         * - Pull the nested payment object out of the webhook body.
         *
         * FLOW:
         * 1. Read the "obj" key from the payload.
         * 2. Throw IllegalArgumentException when it is absent.
         * 3. Return it.
         *
         * NOTE:
         * - The cast is unchecked, so a non-map value under "obj"
         * causes a ClassCastException instead of the intended
         * IllegalArgumentException.
         *
         * @param payload Raw webhook body.
         *
         * @return The map stored under "obj".
         */
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

    /*
         * HANDLE PAYMENT SUCCESS
         *
         * PURPOSE:
         * - Mark a payment as completed once Paymob confirms it.
         * - Announce the result so the account owner is notified.
         *
         * FLOW:
         * 1. Read the order id from the nested order object.
         * 2. Reject the payload when the order information is missing.
         * 3. Read the Paymob transaction id.
         * 4. Load the local payment by Paymob order id.
         * 5. Set the status to COMPLETED and store the transaction id.
         * 6. Save and publish payment.completed.
         *
         * NOTE:
         * - Nothing is debited or credited on the caller's account
         * here; the event only informs notification-service.
         *
         * @param paymentData The "obj" map of a successful webhook.
         */
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
         * - Mark a payment as failed after Paymob reports it.
         * - Announce the failure so the account owner is notified.
         *
         * FLOW:
         * 1. Read the order id from the nested order object.
         * 2. Reject the payload when the order information is missing.
         * 3. Load the local payment by Paymob order id.
         * 4. Set the status to FAILED with a fixed reason.
         * 5. Save and publish payment.failed.
         *
         * NOTE:
         * - Every failure gets the same hardcoded reason; nothing from
         * the payload is stored.
         * - The whole body is wrapped in a try/catch that logs and
         * swallows exceptions, so unlike the success path a failure is
         * never reported back to Paymob and never retried.
         *
         * @param paymentData The "obj" map of a failed webhook.
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

    /*
         * GET PAYMENT BY PAYMOB TRANSACTION ID
         *
         * PURPOSE:
         * - Retrieve a payment using the id Paymob assigned to the
         * transaction.
         *
         * FLOW:
         * 1. Look the payment up by the stored Paymob transaction id.
         * 2. Throw a RuntimeException when no row matches.
         * 3. Return the entity.
         *
         * @param transactionId Paymob transaction id to look up.
         *
         * @return The matching Payment entity.
     */
    public Payment getPaymentByTransactionId(String transactionId) {

        return paymentRepository
                .findByPaymobTransactionId(transactionId)
                .orElseThrow(()
                        -> new RuntimeException(
                        "Payment not found for transaction: "
                        + transactionId));
    }

}
