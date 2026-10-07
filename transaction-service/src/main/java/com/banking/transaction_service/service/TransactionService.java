package com.banking.transaction_service.service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.banking.transaction_service.client.AccountServiceClient;
import com.banking.transaction_service.dto.TransactionResponse;
import com.banking.transaction_service.dto.TransferRequest;
import com.banking.transaction_service.entity.Transaction;
import com.banking.transaction_service.entity.TransactionStatus;
import com.banking.transaction_service.entity.TransactionType;
import com.banking.transaction_service.event.TransactionEventConsumer;
import com.banking.transaction_service.event.TransactionInitiatedEvent;
import com.banking.transaction_service.repository.TransactionRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, Object> redisTemplate;
    private static final String TRANSACTION_INTIATED_TOPIC = "transaction.initiated";

    private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";

    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";

    private static final String FRAUD_DETECTED_TOPIC = "fraud.detected";

    /*
     * SAGA STEP 1 - INITIATE TRANSFER
     *
     * PURPOSE:
     * - Open a new transfer transaction between two accounts.
     * - Deduct the amount from the sender account.
     * - Publish the event that triggers the fraud check.
     *
     * FLOW:
     * 1. Build a Transaction entity with status PENDING and a random
     * reference number, then save it to get its generated id.
     * 2. Call account-service to deduct the amount from the sender.
     * 3. Flip the status to PROCCESSING and save again.
     * 4. Publish transaction.initiated on Kafka, keyed by transaction id.
     * 5. Return the transaction as it currently stands.
     *
     * NOTE:
     * - No OTP is generated here. The OTP is only produced when the
     * fraud check reports the transaction as suspicious.
     * - The response is returned before the fraud check runs, so the
     * caller normally observes status PROCCESSING, not the final state.
     * - The receiver is not credited here. The credit happens later,
     * asynchronously, when account-service consumes
     * transaction.completed.
     *
     * @param request Sender account, receiver account, amount and
     * description of the transfer.
     *
     * @return TransactionResponse reflecting status PROCCESSING.
     */
    public TransactionResponse transfer(TransferRequest request) {

        log.info("SAGA START - Transfer : {} -> {} : amount {}", request.getSenderAccountNumber(),
                request.getReceiverAccountNumber(), request.getAmount());

        Transaction transaction = Transaction.builder()
                .receiverAccountNumber(request.getReceiverAccountNumber())
                .senderAccountNumber(request.getSenderAccountNumber())
                .amount(request.getAmount())
                .transactionStatus(TransactionStatus.PENDING)
                .transactionType(TransactionType.TREANSFER)
                .description(request.getDescription())
                .referenceNumber(UUID.randomUUID().toString())
                .reasoneFailure("N/A")
                .build();

        transactionRepository.save(transaction);

        // SAGA STEP 1 - deduct from sender
        accountServiceClient.deductBalance(request.getSenderAccountNumber(), request.getAmount());

        // Convert Proccess Type form PENDING ----to-----> PROCCESSING
        transaction.setTransactionStatus(TransactionStatus.PROCCESSING);
        transactionRepository.save(transaction);
        log.info("Transaction saved as PROCCESSING ==> {} ", transaction.getId());

        /*
         * SAGA STEP - 2 : PUBLISH FOR FRAUD CHECK
         */
        TransactionInitiatedEvent eventService = TransactionInitiatedEvent.builder()
                .amount(transaction.getAmount())
                .receiverAccountNumber(transaction.getReceiverAccountNumber())
                .senderAccountNumber(transaction.getSenderAccountNumber())
                .description(transaction.getDescription())
                .transactionId(transaction.getId())
                .build();

        kafkaTemplate.send(TRANSACTION_INTIATED_TOPIC, transaction.getId(), eventService);
        log.info("SAGA STEP 2 - TransactionInitiatedEvent publish : {} ", transaction.getId());

        return mapToResponse(transaction);
    }

    /*
     * GET TRANSACTION BY ID
     *
     * PURPOSE:
     * - Retrieve a single transaction using its id.
     *
     * FLOW:
     * 1. Look the transaction up by primary key.
     * 2. Throw a RuntimeException if no row matches.
     * 3. Map the entity to a TransactionResponse.
     *
     * NOTE:
     * - Throws an unchecked RuntimeException, so an unknown id
     * surfaces as HTTP 500 rather than 404.
     *
     * @param transferId Primary key of the transaction. Despite the
     * name, this is the transaction id and not the
     * reference number.
     *
     * @return TransactionResponse of the matching transaction.
     */
    public TransactionResponse getTransaction(String transferId) {

        return mapToResponse(transactionRepository.findById(transferId)
                .orElseThrow(() -> new RuntimeException("Transaction Not Found")));

    }

    /*
     * GET ACCOUNT TRANSACTION HISTORY
     *
     * PURPOSE:
     * - List every transaction in which the account took part,
     * as sender or as receiver.
     *
     * FLOW:
     * 1. Query by sender account number OR receiver account number.
     * 2. Map each Transaction entity to a TransactionResponse.
     *
     * NOTE:
     * - No date range or status filter is applied, so the result
     * includes PENDING, PROCCESSING, PENDING_VERIFICATION,
     * COMPLETED and FLAGGED transactions alike.
     *
     * @param accountNumber Account number to look up in both
     * the sender and the receiver columns.
     *
     * @return List of TransactionResponse, empty when the account
     * has no transactions.
     */
    public List<TransactionResponse> getTransactionHistory(String accountNumber) {

        List<Transaction> transactions = transactionRepository
                .findBySenderAccountNumberOrReceiverAccountNumber(accountNumber);

        return transactions.stream().map(this::mapToResponse).toList();
    }

    public List<TransactionResponse> getAllTransaction(String accountNumber) {

        List<Transaction> transactions = transactionRepository
                .findAllTransactionsBySenderAccountNumber(accountNumber);

        return transactions.stream().map(this::mapToResponse).toList();
    }

    /*
     * SAGA STEP 3 - VERIFY OTP
     *
     * PURPOSE:
     * - Verify the OTP that was generated for a transaction
     * that the fraud check flagged as suspicious.
     *
     * FLOW:
     * 1. Load the transaction from the database.
     * 2. Reject the call unless the status is PENDING_VERIFICATION.
     * 3. Read the expected OTP from Redis using the key
     * "verification:otp:" + transactionId.
     *
     * OTP EXPIRED OR MISSING:
     * - Compensate the transaction and refund the sender.
     *
     * WRONG OTP:
     * - Delete the OTP from Redis.
     * - Publish fraud.detected so account-service blocks the sender.
     * - Compensate the transaction and refund the sender.
     *
     * CORRECT OTP:
     * - Delete the OTP from Redis.
     * - Complete the transaction.
     *
     * NOTE:
     * - An OTP only exists for transactions that entered the
     * PENDING_VERIFICATION state. Transactions that the fraud
     * check cleared never get one and are completed without
     * ever reaching this method.
     * - The PENDING_VERIFICATION guard makes this method safe to
     * call on an already completed or refunded transaction: it
     * throws instead of compensating a second time.
     * - The OTP key has a 5 minute TTL, so a missing key means
     * either expired or already consumed.
     *
     * @param transactionId Id of the transaction being verified.
     *
     * @param otp One-time code the user submitted. Compared with
     * equals against the value stored in Redis.
     *
     * @return TransactionResponse after the outcome was applied:
     * COMPLETED on success, FLAGGED on wrong or expired OTP.
     */
    public TransactionResponse verifyOTP(String transactionId, String otp) {

        log.info("OTP Verification for the transaction : {} ", transactionId);
        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new RuntimeException("Transaction Not Found " + transactionId));

        if (transaction.getTransactionStatus() != TransactionStatus.PENDING_VERIFICATION) {
            throw new RuntimeException(
                    "Transaction is not waiting for OTP verification: " + transactionId);
        }

        String otpKey = "verification:otp:" + transactionId;
        String storeOtp = (String) redisTemplate.opsForValue().get(otpKey);

        // OTP expired or doesn't exist
        if (storeOtp == null) {

            log.warn("OTP expired for transaction :{} ", transactionId);
            compansateTransaction(transaction, "OTP expired - transaction cancelled and amount refunded");
            return mapToResponse(transaction);

        }

        // Wrong OTP
        if (!storeOtp.equals(otp)) {
            log.warn("Wrong OTP - Blocking Account and refunding  : {} ", transactionId);
            redisTemplate.delete(otpKey);
            blockedAccountAndCompansate(transaction,
                    "Wrong OTP entered - transaction cancelled  "
                            + "account blocked for security");
            return mapToResponse(transaction);
        }

        // Correct OTP
        log.info("OTP Verification - Completing Verification transaction : {} ", transactionId);

        redisTemplate.delete(otpKey);

        completeTransactionResponse(transaction);
        return mapToResponse(transaction);

    }

    /*
     * SAGA COMPLETION - COMPLETE TRANSACTION
     *
     * PURPOSE:
     * - Mark the transaction as successfully finished.
     * - Announce the success so the receiver gets credited.
     *
     * FLOW:
     * 1. Set status to COMPLETED.
     * 2. Stamp completedAt with the current time.
     * 3. Save the updated transaction.
     * 4. Publish transaction.completed on Kafka, keyed by
     * transaction id.
     *
     * NOTE:
     * - This method does NOT credit the receiver. The direct
     * creditBalance call is commented out in the body. The credit
     * is performed by account-service when it consumes
     * transaction.completed, so it happens asynchronously and
     * after this transaction is already marked COMPLETED.
     * - There is no status guard here. The caller is responsible
     * for only ever invoking this on a transaction that is still
     * in progress.
     *
     * @param transaction The transaction to complete. Its status
     * and completedAt are mutated in place
     * before saving.
     */
    private void completeTransactionResponse(Transaction transaction) {
        log.info("SAGA COMPLETION - refunding : {} Amount {} ", transaction.getSenderAccountNumber(),
                transaction.getAmount());

        // accountServiceClient.creditBalance(transaction.getSenderAccountNumber(),
        // transaction.getAmount());
        transaction.setTransactionStatus(TransactionStatus.COMPLETED);
        transaction.setCompletedAt(LocalDateTime.now());
        transactionRepository.save(transaction);

        TransactionEventConsumer transactionEventConsumer = TransactionEventConsumer.builder()
                .id(transaction.getId())
                .amount(transaction.getAmount())
                .description(transaction.getDescription())
                .senderAccountNumber(transaction.getSenderAccountNumber())
                .receiverAccountNumber(transaction.getReceiverAccountNumber())
                .build();

        kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC, transaction.getId(), transactionEventConsumer);
        log.info("SAGA COMPLETE - Transaction {} COMPLETED ", transaction.getId());
    }

    /*
     * SAGA COMPENSATION - WRONG OTP
     *
     * PURPOSE:
     * - Handle a security failure caused by an incorrect OTP.
     *
     * FLOW:
     * 1. Publish fraud.detected on Kafka with the sender account
     * number, so account-service blocks that account.
     * 2. Run the Saga compensation to refund the sender.
     *
     * NOTE:
     * - The block and the refund are not atomic. fraud.detected is
     * published first and consumed asynchronously, while the refund
     * runs synchronously in this thread. If the block lands before
     * the credit call, account-service rejects the credit because
     * the account is no longer ACTIVE, and the refund is lost.
     *
     * @param transaction Transaction to compensate.
     *
     * @param reason Human readable explanation stored on the
     * transaction and sent in both Kafka events.
     */
    private void blockedAccountAndCompansate(Transaction transaction, String reason) {

        // Publish Fraud Detected -> Account Service will block Account
        Map<String, Object> fraudEvent = new HashMap<>();

        fraudEvent.put("transactionId", transaction.getId());
        fraudEvent.put("accountNumber", transaction.getSenderAccountNumber());

        fraudEvent.put("reason", reason);
        kafkaTemplate.send(FRAUD_DETECTED_TOPIC, transaction.getId(), fraudEvent);

        log.warn("fraud detected published - account : {} will be blocked , kindly contact to the bank",
                transaction.getSenderAccountNumber());

        compansateTransaction(transaction, reason);
    }

    /*
     * SAGA COMPENSATION - REFUND TRANSACTION
     *
     * PURPOSE:
     * - Roll back the debit that transfer() already performed
     * when the transaction can no longer be completed.
     *
     * FLOW:
     * 1. Credit the amount back to the sender through account-service.
     * 2. Mark the transaction as FLAGGED.
     * 3. Store the reason plus a refund timestamp in reasoneFailure.
     * 4. Save the updated transaction.
     * 5. Publish transaction.refunded on Kafka, keyed by
     * transaction id.
     *
     * NOTE:
     * - The refund event carries: transactionId, senderAccountNumber,
     * amount and reason. The notification service consumes all four.
     * - The credit is synchronous and has no idempotency guard, so
     * calling this twice for one transaction refunds twice.
     *
     * @param transaction Transaction to compensate.
     *
     * @param reason Human readable explanation stored on the
     * transaction and sent in the refund event.
     */
    private void compansateTransaction(Transaction transaction, String reason) {

        log.warn("SAGA COMPENSATION - refunding : {} Amount {} ", transaction.getSenderAccountNumber(),
                transaction.getAmount());

        // CREADIT MONY BACK TO SENDER SYNCRONOUSLY
        accountServiceClient.creditBalance(transaction.getSenderAccountNumber(), transaction.getAmount());
        transaction.setTransactionStatus(TransactionStatus.FLAGGED);
        transaction.setReasoneFailure(reason
                + "SAGA Compensation Execuded , Amount refunded at " + LocalDateTime.now());
        transactionRepository.save(transaction);

        // PUBLISH refuned event - Notification service will alert user
        Map<String, Object> refundEvent = new HashMap<>();
        refundEvent.put("transactionId", transaction.getId());
        refundEvent.put("amount", transaction.getAmount());
        refundEvent.put("senderAccountNumber", transaction.getSenderAccountNumber());
        refundEvent.put("reason", reason);

        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC, transaction.getId(), refundEvent);
        log.info("SAGA COMPENSATION COMPLETE - {} refunded to {} ", transaction.getAmount(),
                transaction.getSenderAccountNumber());

    }

    /*
     * MAP TRANSACTION TO RESPONSE
     *
     * PURPOSE:
     * - Convert a Transaction entity into its API representation.
     *
     * FLOW:
     * 1. Copy every field across to the response builder.
     * 2. Return the built TransactionResponse.
     *
     * NOTE:
     * - createdAt exists on the entity but is not copied, so it always
     * comes back null in the response.
     * - completedAt is set twice on the builder; both calls pass the
     * same value, so the duplicate has no effect.
     *
     * @param transaction Entity to map.
     *
     * @return TransactionResponse built from the entity.
     */
    private TransactionResponse mapToResponse(Transaction transaction) {
        return TransactionResponse.builder()
                .id(transaction.getId())
                .receiverAccountNumber(transaction.getReceiverAccountNumber())
                .senderAccountNumber(transaction.getSenderAccountNumber())
                .amount(transaction.getAmount())
                .description(transaction.getDescription())
                .transactionStatus(transaction.getTransactionStatus())
                .transactionType(transaction.getTransactionType())
                .reasoneFailure(transaction.getReasoneFailure())
                .referenceNumber(transaction.getReferenceNumber())
                .completedAt(transaction.getCompletedAt())
                .completedAt(transaction.getCompletedAt())
                .build();
    }

    /*
     * SAGA CONTINUATION - PROCESS CLEAN FRAUD RESULT
     *
     * Triggered by:
     * - The fraud.check.clean Kafka event.
     *
     * PURPOSE:
     * - Continue the Saga for a transaction the fraud check cleared.
     *
     * FLOW:
     * 1. Load the transaction from the database.
     * 2. If the status is not PROCCESSING, log and return.
     * 3. Complete the transaction.
     *
     * NOTE:
     * - This path completes a transfer WITHOUT any OTP. It runs
     * when the fraud check reports the transaction as clean, and
     * it never reads Redis. An OTP is only involved when the fraud
     * check flags the transaction, which sends it to
     * PENDING_VERIFICATION instead.
     * - The only guard is the PROCCESSING check. It stops a
     * transaction that was already cancelled, refunded or
     * completed from being completed again, but it does not verify
     * the OTP.
     *
     * @param transactionId Id of the transaction to continue.
     */
    public void processCleanResult(String transactionId) {
        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new RuntimeException("Transaction Not Found " + transactionId));

        if (transaction.getTransactionStatus() != TransactionStatus.PROCCESSING) {
            log.warn("Transaction {} not PROCESSING -skipping ", transactionId);
            return;

        }

        completeTransactionResponse(transaction);

    }
}
