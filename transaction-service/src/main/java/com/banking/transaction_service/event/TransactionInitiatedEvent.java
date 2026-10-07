package com.banking.transaction_service.event;

import java.math.BigDecimal;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TransactionInitiatedEvent {

    private String transactionId;

    private String senderAccountNumber;

    private String receiverAccountNumber;

    private BigDecimal amount;

    private String description;

}
