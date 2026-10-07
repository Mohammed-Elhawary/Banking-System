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

    /*
     * POST /api/v1/payments/create-order
     *
     * PURPOSE:
     * - Expose payment order creation to the frontend.
     *
     * FLOW:
     * 1. Validate the request body.
     * 2. Delegate to the service layer.
     * 3. Return 201 CREATED with the Paymob checkout details.
     *
     * @param request Validated account number, amount and description.
     *
     * @return 201 with the payment id, client secret and public key the
     *         frontend needs to open Paymob Unified Checkout.
     */
    @PostMapping("/create-order")
    public ResponseEntity<PaymentOrderResponse> createPaymentOrder(
            @Valid @RequestBody CreatePaymentRequest request) {

        return ResponseEntity.status(HttpStatus.CREATED).body(paymentService.createPaymentOrder(request));
    }

/*
     * POST /api/v1/payments/webhook
     *
     * PURPOSE:
     * - Receive Paymob's callback after the user pays.
     *
     * FLOW:
     * 1. Read the HMAC query parameter and the JSON body.
     * 2. Delegate verification and processing to the service layer.
     * 3. Return 200 with a plain acknowledgement.
     *
     * NOTE:
     * - The hmac parameter is optional here. When it is absent the
     * comparison fails and the service raises a SecurityException.
     * - A rejected callback surfaces as HTTP 500, not 401.
     *
     * @param hmac    Signature Paymob computed over the payload.
     * @param payload Raw webhook body.
     *
     * @return 200 once the callback was accepted and processed.
     */
    @PostMapping("/webhook")
    public ResponseEntity<String> handleWebhook(
            @RequestParam(required = false) String hmac,
            @RequestBody Map<String, Object> payload) {

        paymentService.handleWebhook(payload, hmac);

        return ResponseEntity.ok("Webhook received");
    }

    /*
     * GET /api/v1/payments/payment-result
     *
     * PURPOSE:
     * - Render the outcome page Paymob redirects the user to after
     * checkout.
     *
     * FLOW:
     * 1. Look the payment up by Paymob transaction id.
     * 2. Render an inline HTML page with the payment id, amount and
     * status.
     *
     * NOTE:
     * - This is a GET that renders raw HTML built by string formatting,
     * with no escaping of the values it prints.
     *
     * @param id Paymob transaction id to look up.
     *
     * @return 200 with the HTML result page.
     */
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
