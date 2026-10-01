package com.banking.transaction_service.entity;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import org.hibernate.annotations.CreationTimestamp;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "transactions")
public class Transaction {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String id;

    @Column(nullable = false)
    private String senderAccountNumber;
    @Column(nullable = false)

    private String receiverAccountNumber;
    @Column(nullable = false, precision = 15, scale = 2)

    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)

    private TransactionType transactionType;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)

    private TransactionStatus transactionStatus;
    @Column(nullable = false)

    private String reasoneFailure;

    private String description;

    private String referenceNumber;

    @CreationTimestamp
    private LocalDateTime createdAt;

    private LocalDateTime completedAt;  

}
