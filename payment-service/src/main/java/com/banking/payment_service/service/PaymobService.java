package com.banking.payment_service.service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import com.banking.payment_service.config.PaymobProperties;
import com.banking.payment_service.dto.CreateIntentionRequest;
import com.banking.payment_service.dto.PaymobIntentionResponse;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class PaymobService {

    private final RestClient paymobRestClient;
    private final PaymobProperties properties;

    /*
     * CREATE PAYMOB INTENTION
     *
     * PURPOSE:
     * - Create a payment intention in Paymob through their HTTP API.
     *
     * FLOW:
     * 1. Convert the amount to the smallest currency unit.
     * 2. Build the request body with amount, currency, payment method
     * and special reference.
     * 3. POST it to /v1/intention/ with the secret key as a token.
     * 4. Return Paymob's deserialized response.
     *
     * NOTE:
     * - No billing data, notification URL or redirection URL is sent
     * here, unlike createPaymentOrder in PaymentService which does.
     *
     * @param request Amount, currency and special reference.
     *
     * @return PaymobIntentionResponse with the ids and client secret.
     */
    public PaymobIntentionResponse createIntention(
            CreateIntentionRequest request) {

        int amountInCents = request.amount()
                .multiply(BigDecimal.valueOf(100))
                .intValue();

        Map<String, Object> body = new HashMap<>();

        body.put("amount", amountInCents);
        body.put("currency", request.currency());
        body.put("payment_methods",
                List.of(Integer.valueOf(properties.integrationId())));
        body.put("special_reference", request.specialReference());

        return paymobRestClient.post()
                .uri("/v1/intention/")
                .header(
                        HttpHeaders.AUTHORIZATION,
                        "Token " + properties.secretKey()
                )
                .contentType(MediaType.APPLICATION_JSON)
                .body(body)
                .retrieve()
                .body(PaymobIntentionResponse.class);
    }
}
