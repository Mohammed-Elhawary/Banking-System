package com.banking.account_service.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.banking.account_service.entities.Account;

@Repository 
public interface AccountRepository extends JpaRepository<Account, String> {

    boolean existsByEmail(String string);

    boolean existsByAccountNumber(String accountNumber);

    Optional<Account> findByAccountNumber(String accountNumber);
    
}
