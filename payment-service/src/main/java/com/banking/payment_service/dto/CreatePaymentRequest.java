package com.banking.payment_service.dto;

import java.math.BigDecimal;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class CreatePaymentRequest {

    @NotBlank(message = "Account Number is required")
    private String accountNumber;

    @NotNull(message = "Amount Number is required")
    @Positive(message = "Amount must be positive")
    private BigDecimal amount;

    private String description;

}
