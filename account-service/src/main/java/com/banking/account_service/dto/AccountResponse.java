package com.banking.account_service.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import com.banking.account_service.entities.AccountStatus;
import com.banking.account_service.entities.AccountType;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class AccountResponse {

    private String id;

    private String accountNumber;

    private String accountHolderName;

    private String email;

    private String phone;

    private AccountType accountType;

    private AccountStatus accountStatus;

    private BigDecimal balance;

    private BigDecimal dailyTransactionLimit;

    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;

}
