package com.banking.account_service.controller;

import java.math.BigDecimal;
import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.banking.account_service.dto.AccountResponse;
import com.banking.account_service.dto.CreateAccountRequest;
import com.banking.account_service.service.AccountService;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/v1/accounts")
@RequiredArgsConstructor
public class AccountController {

    private final AccountService accountService;

    /*
     * POST /api/v1/accounts/create
     *
     * PURPOSE:
     * - Expose account creation.
     *
     * FLOW:
     * 1. Validate the request body.
     * 2. Delegate to the service layer.
     * 3. Return 201 CREATED with the new account.
     *
     * @param request Validated holder details and opening deposit.
     *
     * @return 201 with the created account.
     */
    @PostMapping("/create")
    public ResponseEntity<AccountResponse> createAccount(
            @Valid @RequestBody CreateAccountRequest request) {

        return ResponseEntity.status(HttpStatus.CREATED).body(accountService.createAccount(request));
    }

    ;

    /*
     * GET /api/v1/accounts/{accountNumber}
     *
     * PURPOSE:
     * - Expose the full details of one account.
     *
     * FLOW:
     * 1. Delegate to the service layer.
     * 2. Return 200 with the account.
     *
     * @param accountNumber Account number to look up.
     *
     * @return 200 with the account, or 404 when it does not exist.
     */
    @GetMapping("/{accountNumber}")
    public ResponseEntity<AccountResponse> getAccount(@PathVariable String accountNumber) {
        return ResponseEntity.ok(accountService.getAccount(accountNumber));

    }

    /*
     * GET /api/v1/accounts/
     *
     * PURPOSE:
     * - Expose the full account list.
     *
     * FLOW:
     * 1. Delegate to the service layer.
     * 2. Return 200 with every account.
     *
     * @return 200 with the unpaginated list of accounts.
     */
    @GetMapping("/")
    public ResponseEntity<List<AccountResponse>> getAllAccount() {
        return ResponseEntity.ok(accountService.getAllAccount());

    }

    /*
     * GET /api/v1/accounts/{accountNumber}/balance
     *
     * PURPOSE:
     * - Expose the balance of one account.
     *
     * FLOW:
     * 1. Delegate to the service layer.
     * 2. Return 200 with the balance value.
     *
     * @param accountNumber Account number to look up.
     *
     * @return 200 with the balance, or 404 when the account is unknown.
     */
    @GetMapping("/{accountNumber}/balance")
    public ResponseEntity<BigDecimal> GetBalance(@PathVariable String accountNumber) {
        return ResponseEntity.ok(accountService.getBalance(accountNumber));

    }

    /*
     * PATCH /api/v1/accounts/{accountNumber}/block
     *
     * PURPOSE:
     * - Expose manual account blocking.
     *
     * FLOW:
     * 1. Delegate to the service layer.
     * 2. Return 200 with a confirmation message.
     *
     * NOTE:
     * - The same state change also happens through the fraud.detected
     * Kafka event; there is no authentication on this endpoint.
     *
     * @param accountNumber Account number to block.
     *
     * @return 200 with a success message.
     */
    @PatchMapping("/{accountNumber}/block")
    public ResponseEntity<String> BlockAccount(@PathVariable String accountNumber) {

        accountService.blockAccount(accountNumber);
        return ResponseEntity.ok("Account blocked successfully");

    }
    /*
     * PATCH /api/v1/accounts/{accountNumber}/active
     *
     * PURPOSE:
     * - Expose manual account activation.
     *
     * FLOW:
     * 1. Delegate to the service layer.
     * 2. Return 200 with a confirmation message.
     *
     * @param accountNumber Account number to activate.
     *
     * @return 200 with a success message.
     */
    @PatchMapping("/{accountNumber}/active")
    public ResponseEntity<String> ActiveAccount(@PathVariable String accountNumber) {

        accountService.ActiveAccount(accountNumber);
        return ResponseEntity.ok("Account activated successfully");

    }

    /*
     * SAGA STEP 1 - DEDUCT BALANCE
     *
     * Called by:
     * - Transaction-service, when a transfer is initiated.
     *
     * PURPOSE:
     * - Expose the debit side of the Saga to the transaction service.
     *
     * FLOW:
     * 1. Delegate to the service layer.
     * 2. Return 200 with a confirmation message.
     *
     * NOTE:
     * - Declared as POST although it only changes state, and the
     * return type is a plain string rather than a DTO.
     *
     * @param accountNumber The sender account to debit.
     *
     * @param amount        Amount to subtract.
     *
     * @return 200 on success; 422 when the balance is too low and 403
     *         when the account is not active.
     */
    @PostMapping("/{accountNumber}/deduct")
    public ResponseEntity<String> DeductBalance(@PathVariable String accountNumber, @RequestParam BigDecimal amount) {
        accountService.deductBalance(accountNumber, amount);
        return ResponseEntity.ok("Blance Deducted successfully");
    }

    /*
     * SAGA COMPENSATION - CREDIT BALANCE
     *
     * Called by:
     * - Transaction-service, when a transaction is compensated and the
     * sender must be refunded.
     *
     * PURPOSE:
     * - Expose the credit side of the Saga to the transaction service.
     *
     * FLOW:
     * 1. Delegate to the service layer.
     * 2. Return 200 with a confirmation message.
     *
     * NOTE:
     * - Crediting the receiver does NOT go through this endpoint. That
     * happens in-process when this service consumes the
     * transaction.completed Kafka event.
     * - There is no idempotency key, so a repeated call credits twice.
     *
     * @param accountNumber The account to credit.
     *
     * @param amount        Amount to add.
     *
     * @return 200 on success; 403 when the account is blocked or not
     *         active, and 400 for a non-positive amount.
     */
    @PostMapping("/{accountNumber}/credit")
    public ResponseEntity<String> creditBalance(@PathVariable String accountNumber, @RequestParam BigDecimal amount) {

        accountService.creditBalance(accountNumber, amount);

        return ResponseEntity.ok("Balance credit successfully");
    }

}
