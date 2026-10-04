package com.banking.payment_service.dto;

import java.math.BigDecimal;
import java.util.List;

public record CreateIntentionRequest(
        BigDecimal amount,
        String currency,
        List<Integer> paymentMethods,
        String specialReference
        ) {

}
