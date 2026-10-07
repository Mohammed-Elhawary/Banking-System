package com.banking.account_service.service;

import java.math.BigDecimal;
import java.util.List;

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
         * CREATE ACCOUNT
         *
         * PURPOSE:
         * - Open a new customer account.
         *
         * FLOW:
         * 1. Reject the request if the email is already registered.
         * 2. Build the Account with status ACTIVE.
         * 3. Generate a unique account number based on the type.
         * 4. Apply a default daily limit: 100000 for SAVING,
         * 500000 otherwise.
         * 5. Set the opening balance to the initial deposit.
         * 6. Save and return the account as a response DTO.
         *
         * NOTE:
         * - The dailyTransactionLimit is set here but never enforced
         * anywhere else in the service.
         *
         * @param request Holder name, email, phone, type and opening
         *                deposit of the new account.
         *
         * @return AccountResponse of the freshly created account.
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
         * GET ACCOUNT BY ACCOUNT NUMBER
         *
         * PURPOSE:
         * - Retrieve the full details of one account.
         *
         * FLOW:
         * 1. Look the account up by its account number.
         * 2. Throw AccountNotFoundException when no row matches.
         * 3. Map the entity to an AccountResponse.
         *
         * @param accountNumber The account number to look up.
         *
         * @return AccountResponse of the matching account.
         */
    public AccountResponse getAccount(String accountNumber) {

        Account account = accountRepository
                .findByAccountNumber(accountNumber)
                .orElseThrow(() -> new AccountNotFoundException(
                "Account not found: " + accountNumber));
        return mapToResponse(account);
    }

    /*
         * GET ACCOUNT BALANCE
         *
         * PURPOSE:
         * - Expose the current balance of an account.
         *
         * FLOW:
         * 1. Look the account up by its account number.
         * 2. Throw AccountNotFoundException when no row matches.
         * 3. Return the balance value.
         *
         * NOTE:
         * - No status check is performed, so a blocked account still
         * reports its balance.
         *
         * @param accountNumber The account number to look up.
         *
         * @return The current balance.
         */
    public BigDecimal getBalance(String accountNumber) {
        Account account = accountRepository
                .findByAccountNumber(accountNumber)
                .orElseThrow(() -> new AccountNotFoundException(
                "Account not found: " + accountNumber));
        return account.getBalance();
    }

/*
         * BLOCK ACCOUNT
         *
         * Triggered by:
         * - Direct API call, or the fraud.detected Kafka event.
         *
         * PURPOSE:
         * - Freeze an account so no further debit or credit can run.
         *
         * FLOW:
         * 1. Look the account up by its account number.
         * 2. Set the status to BLOCKED.
         * 3. Save the account.
         *
         * @param accountNumber The account number to block.
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
         * ACTIVATE ACCOUNT
         *
         * Triggered by:
         * - Direct API call.
         *
         * PURPOSE:
         * - Unfreeze a previously blocked account.
         *
         * FLOW:
         * 1. Look the account up by its account number.
         * 2. Set the status back to ACTIVE.
         * 3. Save the account.
         *
         * @param accountNumber The account number to activate.
         */
    public void ActiveAccount(String accountNumber) {
        Account account = accountRepository
                .findByAccountNumber(accountNumber)
                .orElseThrow(() -> new AccountNotFoundException(
                "Account not found: " + accountNumber));
        account.setAccountStatus(AccountStatus.ACTIVE);
        accountRepository.save(account);
        log.info("Active Account Successfully {} " + accountNumber);
    }

/*
         * SAGA STEP 1 - DEDUCT BALANCE
         *
         * Triggered by:
         * - POST /api/v1/accounts/{accountNumber}/deduct, called by
         * transaction-service when a transfer starts.
         *
         * PURPOSE:
         * - Take the transfer amount out of the sender's balance.
         *
         * FLOW:
         * 1. Reject a null or non-positive amount.
         * 2. Load the account.
         * 3. Reject the operation unless the account is ACTIVE.
         * 4. Reject the operation unless the balance covers the amount.
         * 5. Subtract the amount and save.
         *
         * NOTE:
         * - Read-modify-write without row locking, so two concurrent
         * debits can both pass the balance check.
         *
         * @param accountNumber The sender account to debit.
         *
         * @param amount        Amount to subtract, must be positive.
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
         * CREDIT BALANCE
         *
         * Triggered by:
         * - PATCH /api/v1/accounts/{accountNumber}/credit, called by
         * transaction-service for a Saga refund.
         *
         * PURPOSE:
         * - Add an amount back to an account balance.
         *
         * FLOW:
         * 1. Reject a null or non-positive amount.
         * 2. Load the account.
         * 3. Reject the operation unless the account is ACTIVE.
         * 4. Add the amount and save.
         *
         * NOTE:
         * - Rejecting non-ACTIVE accounts means a refund that races a
         * fraud block fails instead of being applied.
         * - Read-modify-write without row locking, and no idempotency
         * key, so repeated calls credit repeatedly.
         *
         * @param accountNumber The account to credit.
         *
         * @param amount        Amount to add, must be positive.
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
         * MAP ACCOUNT TO RESPONSE
         *
         * PURPOSE:
         * - Convert an Account entity into its API representation.
         *
         * FLOW:
         * 1. Copy every field across to the response builder.
         * 2. Return the built AccountResponse.
         *
         * NOTE:
         * - No password or credential field is copied, so no secret
         * can leak through this DTO.
         *
         * @param savedAccount Entity to map.
         *
         * @return AccountResponse built from the entity.
         */
    private AccountResponse mapToResponse(Account savedAccount) {
        return AccountResponse.builder()
                .accountHolderName(savedAccount.getAccountHolderName())
                .accountNumber(savedAccount.getAccountNumber())
                .accountStatus(savedAccount.getAccountStatus())
                .accountType(savedAccount.getAccountType())
                .phone(savedAccount.getPhone())
                .balance(savedAccount.getBalance())
                .email(savedAccount.getEmail())
                .dailyTransactionLimit(savedAccount.getDailyTransactionLimit())
                .id(savedAccount.getId())
                .createdAt(savedAccount.getCreatedAt())
                .updatedAt(savedAccount.getUpdatedAt())
                .build();
    }

    /*
         * GET ALL ACCOUNTS
         *
         * PURPOSE:
         * - List every account in the system.
         *
         * FLOW:
         * 1. Load all rows from the account table.
         * 2. Map each entity to an AccountResponse.
         *
         * NOTE:
         * - Unfiltered and unpaginated, so this grows without limit.
         *
         * @return List of every account, empty when none exist.
         */
    public List<AccountResponse> getAllAccount() {

        List<Account> accounts = accountRepository.findAll();
        return accounts.stream()
                .map(this::mapToResponse)
                .toList();
    }
}
