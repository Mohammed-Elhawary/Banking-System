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

    private String razorpayOrderId;

    private String razorpayKeyId;

    private BigDecimal amount;

    private String currency;

    private String PaymentStatus;

}
