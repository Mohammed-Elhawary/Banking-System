    package com.banking.account_service.entities;

    import java.math.BigDecimal;
    import java.time.LocalDateTime;

    import org.hibernate.annotations.CreationTimestamp;
    import org.hibernate.annotations.UpdateTimestamp;

    import jakarta.persistence.Column;
    import jakarta.persistence.Entity;
    import jakarta.persistence.EnumType;
    import jakarta.persistence.Enumerated;
    import jakarta.persistence.GeneratedValue;
    import jakarta.persistence.GenerationType;
    import jakarta.persistence.Id;
    import jakarta.persistence.Table;
    import lombok.AllArgsConstructor;
    import lombok.Builder;
    import lombok.Data;
    import lombok.NoArgsConstructor;

    @Entity
    @Table(name = "accounts")
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public class Account {

        @Id
        @GeneratedValue(strategy = GenerationType.UUID)
        private String id;

        @Column(nullable = false, unique = true)
        private String accountNumber;

        @Column(nullable = false)
        private String accountHolderName;

        @Column(nullable = false, unique = true)
        private String email;

        @Column(nullable = false)
        private String phone;

        @Enumerated(EnumType.STRING)
        @Column(nullable = false)
        private AccountType accountType;

        @Enumerated(EnumType.STRING)
        @Column(nullable = false)
        private AccountStatus accountStatus;

        @Column(nullable = false, precision = 15, scale = 2)
        private BigDecimal balance;

        @Column(nullable = false, precision = 15, scale = 2)
        private BigDecimal dailyTransactionLimit;

        @CreationTimestamp
        private LocalDateTime createdAt;

        @UpdateTimestamp
        private LocalDateTime updatedAt;

    }
