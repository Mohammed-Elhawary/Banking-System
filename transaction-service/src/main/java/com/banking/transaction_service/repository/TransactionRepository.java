package com.banking.transaction_service.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.banking.transaction_service.entity.Transaction;

@Repository
public interface TransactionRepository extends JpaRepository<Transaction, String> {

    @Query("SELECT t FROM Transaction t WHERE t.senderAccountNumber = :accountNumber "
            + "OR t.receiverAccountNumber = :accountNumber")
    List<Transaction> findBySenderAccountNumberOrReceiverAccountNumber(
            @Param("accountNumber") String accountNumber);

    List<Transaction> findAllTransactionsBySenderAccountNumber(String accountNumber);
}
