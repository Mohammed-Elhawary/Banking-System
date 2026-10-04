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
