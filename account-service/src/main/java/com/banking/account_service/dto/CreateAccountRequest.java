package com.banking.account_service.dto;

import java.math.BigDecimal;

import com.banking.account_service.entities.AccountType;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CreateAccountRequest {

    @NotBlank(message = "Account Holder Name is required")
    private String accountHolderName;

    @NotBlank(message = "Email is required")
    @Email(message = "Invalid email format")
    private String email;

    @NotBlank(message = "phone is required")
    private String phone;

    @NotNull(message = "Account Type is required")
    private AccountType accountType;

    @NotNull(message = "Initial deposit is required")
    @PositiveOrZero(message = "Initial deposit must be positive number")
    private BigDecimal initialDeposit;

}
