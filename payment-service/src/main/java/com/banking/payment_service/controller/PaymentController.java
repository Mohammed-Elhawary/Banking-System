package com.banking.payment_service.controller;

import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.banking.payment_service.dto.CreatePaymentRequest;
import com.banking.payment_service.dto.PaymentOrderResponse;
import com.banking.payment_service.entity.Payment;
import com.banking.payment_service.service.PaymentService;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("api/v1/payments")
@Slf4j
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentService paymentService;

    @PostMapping("/create-order")
    public ResponseEntity<PaymentOrderResponse> createPaymentOrder(
            @Valid @RequestBody CreatePaymentRequest request) {

        return ResponseEntity.status(HttpStatus.CREATED).body(paymentService.createPaymentOrder(request));
    }

    // Paymob webhook endpoint       
    @PostMapping("/webhook")
    public ResponseEntity<String> handleWebhook(
            @RequestParam(required = false) String hmac,
            @RequestBody Map<String, Object> payload) {

        paymentService.handleWebhook(payload, hmac);

        return ResponseEntity.ok("Webhook received");
    }

    @GetMapping("/payment-result")
    public ResponseEntity<String> paymentResult(
            @RequestParam String id) {

        Payment payment
                = paymentService.getPaymentByTransactionId(id);

        return ResponseEntity.ok("""
            <html>
                <body>
                    <h1>Payment Result</h1>
                    <p>Payment ID: %s</p>
                    <p>Amount: %s EGP</p>
                    <p>Status: %s</p>
                </body>
            </html>
            """.formatted(
                payment.getId(),
                payment.getAmount(),
                payment.getPaymentStatus()
        ));
    }
}
