package com.banking.account_service.service;

import java.math.BigDecimal;

import org.springframework.stereotype.Service;

import com.banking.account_service.dto.AccountResponse;
import com.banking.account_service.dto.CreateAccountRequest;
import com.banking.account_service.entities.Account;
import com.banking.account_service.entities.AccountStatus;
import com.banking.account_service.entities.AccountType;
import com.banking.account_service.exception.account.AccountAlreadyExistsException;
import com.banking.account_service.exception.account.AccountNotFoundException;
import com.banking.account_service.exception.account.AccountOperationNotAllowedException;
import com.banking.account_service.exception.balance.InsufficientBalanceException;
import com.banking.account_service.exception.balance.InvalidAmountException;
import com.banking.account_service.helper.AccountNumberGenerator;
import com.banking.account_service.repository.AccountRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountService {

    private final AccountRepository accountRepository;
    private final AccountNumberGenerator accountNumberGenerator;

    /*
     * Create account by account Number
     *
     * @Param accountNumber
     *
     * @return
     */
    public AccountResponse createAccount(CreateAccountRequest request) {

        log.info("Creating account for : {}", request.getEmail());

        if (accountRepository.existsByEmail(request.getEmail())) {

            throw new AccountAlreadyExistsException("Account already exists for email " + request.getEmail());

        }

        Account account = Account.builder()
                .accountHolderName(request.getAccountHolderName())
                .accountType(request.getAccountType())
                .email(request.getEmail())
                .phone(request.getPhone())
                .accountStatus(AccountStatus.ACTIVE)
                .accountNumber(accountNumberGenerator.generate(request.getAccountType()))
                .dailyTransactionLimit(request
                        .getAccountType() == AccountType.SAVING
                                ? new BigDecimal("100000")
                                : new BigDecimal("500000"))
                .balance(request.getInitialDeposit())
                .build();

        Account savedAccount = accountRepository.save(account);
        log.info("Created Account Successfully {} ", savedAccount.getAccountNumber());
        return mapToResponse(savedAccount);
    }

    /*
     * Get account by account Number
     *
     * @Param accountNumber
     *
     * @return
     */
    public AccountResponse getAccount(String accountNumber) {

        Account account = accountRepository
                .findByAccountNumber(accountNumber)
                .orElseThrow(() -> new AccountNotFoundException(
                        "Account not found: " + accountNumber));
        return mapToResponse(account);
    }

    public BigDecimal getBalance(String accountNumber) {
        Account account = accountRepository
                .findByAccountNumber(accountNumber)
                .orElseThrow(() -> new AccountNotFoundException(
                        "Account not found: " + accountNumber));
        return account.getBalance();
    }

    /*
     * Block account - called by Fraud detection Service Via Kafka
     *
     * @Param accountNumber
     */
    public void blockAccount(String accountNumber) {
        Account account = accountRepository
                .findByAccountNumber(accountNumber)
                .orElseThrow(() -> new AccountNotFoundException(
                        "Account not found: " + accountNumber));
        account.setAccountStatus(AccountStatus.BLOCKED);
        accountRepository.save(account);
        log.info("Blocked Account Successfully {} " + accountNumber);
    }

    /*
     * deductBalance from sender Account
     * called by Transaction servies
     *
     * @Param accountNumber
     *
     * @param amount
     */
    public void deductBalance(String accountNumber, BigDecimal amount) {

        log.info(
                "Deducting balance {} from account: {}",
                amount,
                accountNumber);

        if (amount == null || amount.signum() <= 0) {
            throw new InvalidAmountException("Amount must be greater than zero");
        }

        Account account = accountRepository
                .findByAccountNumber(accountNumber)
                .orElseThrow(() -> new AccountNotFoundException("Account not found: " + accountNumber));

        if (account.getAccountStatus() != AccountStatus.ACTIVE) {
            throw new AccountOperationNotAllowedException("Account is not active: " + accountNumber);
        }

        if (account.getBalance().compareTo(amount) < 0) {
            throw new InsufficientBalanceException("Insufficient balance for account " + accountNumber);
        }

        account.setBalance(account.getBalance().subtract(amount));

        accountRepository.save(account);

        log.info("Balance updated successfully: {}", account.getBalance());
    }

    /*
     * creditBalance - called by Fraud detection Service Via Kafka
     *
     * @Param accountNumber
     */
    public void creditBalance(String accountNumber, BigDecimal amount) {
        log.info("Credit balance {} from account: {}", amount, accountNumber);

        if (amount == null || amount.signum() <= 0) {
            throw new InvalidAmountException("Amount must be greater than zero");
        }

        Account account = accountRepository
                .findByAccountNumber(accountNumber)
                .orElseThrow(() -> new AccountNotFoundException("Account not found: " + accountNumber));

        if (account.getAccountStatus() != AccountStatus.ACTIVE) {
            throw new AccountOperationNotAllowedException("Account is not active: " + accountNumber);
        }
        account.setBalance(account.getBalance().add(amount));

        accountRepository.save(account);

        log.info("Balance Credit, New Balance: {}", account.getBalance());

    }

    /*
     * Helper method to convert account Object to Account Response
     *
     * @Param Account Obj
     *
     * @return
     */
    private AccountResponse mapToResponse(Account savedAccount) {
        return AccountResponse.builder()
                .accountHolderName(savedAccount.getAccountHolderName())
                .accountNumber(savedAccount.getAccountNumber())
                .accountStatus(savedAccount.getAccountStatus())
                .accountType(savedAccount.getAccountType())
                .balance(savedAccount.getBalance())
                .email(savedAccount.getEmail())
                .balance(savedAccount.getBalance())
                .dailyTransactionLimit(savedAccount.getDailyTransactionLimit())
                .id(savedAccount.getId())
                .createdAt(savedAccount.getCreatedAt())
                .updatedAt(savedAccount.getUpdatedAt())
                .build();
    }
}
