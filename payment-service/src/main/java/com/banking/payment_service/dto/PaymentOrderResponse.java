package com.banking.payment_service.dto;

import java.math.BigDecimal;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor

@NoArgsConstructor
@Data
@Builder
public class PaymentOrderResponse {

    private String paymentId;

    private String paymobIntentionId;

    private String paymobOrderId;

    private BigDecimal amount;

    private String clientSecret;

    private String publicKey;

    private String currency;

    private String paymentStatus;
}
