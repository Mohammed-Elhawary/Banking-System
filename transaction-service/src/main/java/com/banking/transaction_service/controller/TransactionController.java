package com.banking.transaction_service.controller;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.banking.transaction_service.dto.TransactionResponse;
import com.banking.transaction_service.dto.TransferRequest;
import com.banking.transaction_service.service.TransactionService;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("api/v1/transactions")
@Slf4j
@RequiredArgsConstructor
public class TransactionController {

    private final TransactionService transactionService;

    /*
     * POST /api/v1/transactions/transfer
     *
     * PURPOSE:
     * - Expose the transfer entry point of the Saga.
     *
     * FLOW:
     * 1. Validate the request body.
     * 2. Delegate to the service layer.
     * 3. Return 201 CREATED with the transaction state.
     *
     * @param request Validated sender, receiver, amount and description.
     *
     * @return 201 with the transaction, normally in status PROCCESSING
     *         because the fraud check has not run yet.
     */
    @PostMapping("/transfer")
    public ResponseEntity<TransactionResponse> transfer(@Valid @RequestBody TransferRequest request) {

        return ResponseEntity.status(HttpStatus.CREATED).body(transactionService.transfer(request));
    }

    /*
     * GET /api/v1/transactions/account/{accountNumber}
     *
     * PURPOSE:
     * - Expose the transaction history of one account.
     *
     * FLOW:
     * 1. Delegate to the service layer.
     * 2. Return 200 with the list of transactions.
     *
     * @param accountNumber Account number to look up as sender or receiver.
     *
     * @return 200 with every transaction the account took part in.
     */
    @GetMapping("/account/{accountNumber}")
    public ResponseEntity<List<TransactionResponse>> getTransactionHistory(@PathVariable String accountNumber) {

        return ResponseEntity.ok(transactionService.getTransactionHistory(accountNumber));
    }

    @GetMapping("/All/{accountNumber}")
    public ResponseEntity<List<TransactionResponse>> getAllTransaction(@PathVariable String accountNumber) {

        return ResponseEntity.ok(transactionService.getAllTransaction(accountNumber));
    }

    /*
     * POST /api/v1/transactions/{transactionId}/verify?otp=
     *
     * PURPOSE:
     * - Expose the OTP verification step of the Saga.
     *
     * FLOW:
     * 1. Log the verification attempt.
     * 2. Delegate to the service layer.
     * 3. Return 200 with the resulting transaction state.
     *
     * NOTE:
     * - Only transactions in PENDING_VERIFICATION are accepted here.
     * A transaction the fraud check cleared is already COMPLETED and
     * this call returns an error.
     *
     * @param transactionId Id of the transaction being verified.
     *
     * @param otp           One-time code submitted by the user.
     *
     * @return 200 with COMPLETED on a correct OTP, or FLAGGED after a
     *         refund when the OTP was wrong or had expired.
     */
    @PostMapping("/{transactionId}/verify")
    public ResponseEntity<TransactionResponse> verifyOTP(@PathVariable String transactionId,
            @RequestParam String otp) {
        log.info("OTP verification request - transaction: {}", transactionId);
        return ResponseEntity.ok(transactionService.verifyOTP(transactionId, otp));
    }
}
