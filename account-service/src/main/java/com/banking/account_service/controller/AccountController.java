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

    @PostMapping("/create")
    public ResponseEntity<AccountResponse> createAccount(
            @Valid @RequestBody CreateAccountRequest request) {

        return ResponseEntity.status(HttpStatus.CREATED).body(accountService.createAccount(request));
    }

    ;

    @GetMapping("/{accountNumber}")
    public ResponseEntity<AccountResponse> getAccount(@PathVariable String accountNumber) {
        return ResponseEntity.ok(accountService.getAccount(accountNumber));

    }

    @GetMapping("/")
    public ResponseEntity<List<AccountResponse>> getAllAccount() {
        return ResponseEntity.ok(accountService.getAllAccount());

    }

    @GetMapping("/{accountNumber}/balance")
    public ResponseEntity<BigDecimal> GetBalance(@PathVariable String accountNumber) {
        return ResponseEntity.ok(accountService.getBalance(accountNumber));

    }

    @PatchMapping("/{accountNumber}/block")
    public ResponseEntity<String> BlockAccount(@PathVariable String accountNumber) {

        accountService.blockAccount(accountNumber);
        return ResponseEntity.ok("Account blocked successfully");

    }
    @PatchMapping("/{accountNumber}/active")
    public ResponseEntity<String> ActiveAccount(@PathVariable String accountNumber) {

        accountService.ActiveAccount(accountNumber);
        return ResponseEntity.ok("Account activated successfully");

    }

    /*
     * SAGA Step 1 : Deduct Balance
     * Called By Transaction service when transfer is intiated
     */
    @PostMapping("/{accountNumber}/deduct")
    public ResponseEntity<String> DeductBalance(@PathVariable String accountNumber, @RequestParam BigDecimal amount) {
        accountService.deductBalance(accountNumber, amount);
        return ResponseEntity.ok("Blance Deducted successfully");
    }

    /*
     * SAGA Step 4 : Compensating Transaction Endpoint
     * Called By Transaction Service in Two SCENARIO
     *
     * 1- Fraud detection -> refund sender (undo step 1)
     * 2- Transaction completed -> Credit reciver
     *
     */
    @PostMapping("/{accountNumber}/credit")
    public ResponseEntity<String> creditBalance(@PathVariable String accountNumber, @RequestParam BigDecimal amount) {

        accountService.creditBalance(accountNumber, amount);

        return ResponseEntity.ok("Balance credit successfully");
    }

}
