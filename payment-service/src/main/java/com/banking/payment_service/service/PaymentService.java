package com.banking.payment_service.service;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.banking.payment_service.dto.CreatePaymentRequest;
import com.banking.payment_service.dto.PaymentOrderResponse;
import com.banking.payment_service.entity.Payment;
import com.banking.payment_service.entity.PaymentStatus;
import com.banking.payment_service.repository.PaymentRepository;
import com.razorpay.Order;
import com.razorpay.RazorpayClient;
import com.razorpay.RazorpayException;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private final String PAYMENT_COMPLETED_TOPIC = "payment.completed";
    private final String PAYMENT_FAILED_TOPIC = "payment.failed";

    @Value("${razorpay.key.id}")
    private final String keyId;

    @Value("${razorpay.key.secret}")
    private final String keySecret;

    /*
         * CREATE RAZORPAY PAYMENT ORDER
         *
         * PURPOSE:
         * - Create a new payment order in Razorpay.
         * - Save the payment information in our database.
         * - Return the order details to the frontend.
         *
         * FLOW:
         * 1. Receive payment details from the frontend.
         * 2. Create a payment order in Razorpay.
         * 3. Save the payment record in our database.
         * 4. Return the Razorpay order details to the frontend.
         * 5. Frontend opens Razorpay Checkout.
         * 6. User completes the payment.
         *
         * AFTER PAYMENT:
         * - Razorpay sends the payment result to our Webhook.
         * - Webhook processes the payment status.
         *
         * @param request Payment details received from the frontend.
         *
         * @return PaymentOrderResponse containing the Razorpay order details.
     */
    public PaymentOrderResponse createPaymentOrder(CreatePaymentRequest request) throws RazorpayException {

        log.info("Creating payment order for account : {}  amount : {} ", request.getAccountNumber(),
                request.getAmount());

        RazorpayClient razorpayClient = new RazorpayClient(keyId, keySecret);

        // Convert the amount to the smallest currency unit
        // Example: 1 USD -> 100 cents
        int convertedId = request.getAmount().multiply(BigDecimal.valueOf(100)).intValue();

        JSONObject orderRequest = new JSONObject();

        orderRequest.put("amount", convertedId);
        orderRequest.put("currency", "USD/EGP");
        orderRequest.put("receipt",
                "rcpt_" + System.currentTimeMillis()
                + UUID.randomUUID().toString().replace("_", "").substring(0, 10));

        Order razorpayOrder = razorpayClient.orders.create(orderRequest);

        log.info("Razor Order created {}", razorpayOrder.get("id").toString());

        Payment payment = Payment.builder()
                .accountNumber(request.getAccountNumber())
                .PaymentStatus(PaymentStatus.CREATED)
                .amount(request.getAmount())
                .currency("USD/EGP")
                .razorpayOrderId(razorpayOrder.get("id").toString())
                .description(request.getDescription())
                .build();

        Payment savedPayment = paymentRepository.save(payment);
        return PaymentOrderResponse.builder()
                .paymentId(savedPayment.getId())
                .razorpayKeyId(razorpayOrder.get("id").toString())
                .amount(savedPayment.getAmount())
                .currency(savedPayment.getCurrency())
                .PaymentStatus(PaymentStatus.CREATED.toString())
                .razorpayKeyId(keyId)
                .build();
    }

    public void handleWebhook(Map<String, Object> payload) {
        log.info("Received Razorpay webhook", payload.get("event"));

        String event = (String) payload.get("event");

        if ("payment.captured".equals(event)) {

            handlePaymentSuccess(payload);

        } else if ("payment.failed".equals(event)) {

            handlePaymentFailure(payload);

        }
    }

    private void handlePaymentSuccess(Map<String, Object> payload) {

        try {
            Map<String, Object> paymentData = exteractPaymentData(payload);

            String orderId = (String) paymentData.get("order_id");
            String paymentId = (String) paymentData.get("id");

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId).orElseThrow(
                    () -> new RuntimeException("Payment not found for order-id " + orderId));
            payment.builder().razorpayOrderId(paymentId).PaymentStatus(PaymentStatus.COMPLETED).build();

            paymentRepository.save(payment);

            Map<String, Object> message = Map.of(
                    "paymentId", payment.getId(),
                    "accountNumber", payment.getAccountNumber(),
                    "amount", payment.getAmount(),
                    "razorpayOrderId", paymentId
            );
            kafkaTemplate.send(PAYMENT_COMPLETED_TOPIC, payment.getId(), message);
            log.info("Payment completed for paymentId: {} and orderId: {}", payment.getId(), orderId);

        } catch (Exception e) {
            log.error("Error handling payment success webhook: {}", e.getMessage(), e);
        }
    }

    private void handlePaymentFailure(Map<String, Object> payload) {
        try {
            Map<String, Object> paymentData = exteractPaymentData(payload);

            String orderId = (String) paymentData.get("order_id");
            String paymentId = (String) paymentData.get("id");

            Payment payment = paymentRepository.findByRazorpayOrderId(orderId).orElseThrow(
                    () -> new RuntimeException("Payment not found for order-id " + orderId));
            payment.builder().razorpayOrderId(paymentId)
                    .PaymentStatus(PaymentStatus.FAILED)
                    .failureReason("payment failed via Razorpay webhook")
                    .build();

            paymentRepository.save(payment);
            Map<String, Object> message = Map.of(
                    "paymentId", payment.getId(),
                    "accountNumber", payment.getAccountNumber(),
                    "amount", payment.getAmount(),
                    "reason", payment.getFailureReason()
            );
            kafkaTemplate.send(PAYMENT_FAILED_TOPIC, payment.getId(), message);
            log.info("Payment failed for paymentId: {} and orderId: {}", payment.getId(), orderId);

        } catch (Exception e) {
            log.error("Error handling payment failure webhook: {}", e.getMessage(), e);
        }

    }

    private Map<String, Object> exteractPaymentData(Map<String, Object> payload) {

        Map<String, Object> paymentData = (Map<String, Object>) payload.get("payload");
        Map<String, Object> paymentEntity = (Map<String, Object>) paymentData.get("payment");
        return (Map<String, Object>) paymentEntity.get("entity");
    }

}
