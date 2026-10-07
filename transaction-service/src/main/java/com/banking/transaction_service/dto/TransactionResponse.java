package com.banking.transaction_service.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import com.banking.transaction_service.entity.TransactionStatus;
import com.banking.transaction_service.entity.TransactionType;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder

public class TransactionResponse {
    private String id;
    private String senderAccountNumber;
    private String receiverAccountNumber;
    private BigDecimal amount;
    private TransactionType transactionType;
    private TransactionStatus transactionStatus;
    private String reasoneFailure;
    private String description;
    private String referenceNumber;
    private LocalDateTime createdAt;
    private LocalDateTime completedAt;

}
