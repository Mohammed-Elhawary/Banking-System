package com.banking.transaction_service.event;

import java.math.BigDecimal;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * TransactionEventConsumer
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class TransactionEventConsumer {

    private String id;
    private String senderAccountNumber;
    private String receiverAccountNumber;
    private BigDecimal amount;
    private String description;

}
