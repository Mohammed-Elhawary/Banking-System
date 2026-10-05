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
         * SAGA STEP 1 - initiate transfer
         *
         * @param request
         * deducts from sender via fegin
         *
         * SAVE TRANSACTION AS PROCCESSING
         *
         * PUBLISH EVENT TO KAFKA FOR FRAUD CHECK
         *
         * @return
         *
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
         * Get Transaction By ID
         *
         * @param transferId
         *
         * PURPOSE:
         * - Retrieve a transaction using its ID.
         *
         * FLOW:
         * 1. Search for the transaction in the database.
         * 2. If transaction doesn't exist -> throw exception.
         * 3. Map the entity to TransactionResponse.
         *
         * @return TransactionResponse
     */
    public TransactionResponse getTransaction(String transferId) {

        return mapToResponse(transactionRepository.findById(transferId)
                .orElseThrow(() -> new RuntimeException("Transaction Not Found")));

    }

    /*
         * Get Account Transaction History
         *
         * @param accountNumber
         *
         * PURPOSE:
         * - Retrieve all transactions where the account is
         * either the sender or receiver.
         *
         * FLOW:
         * 1. Search transactions by sender or receiver account number.
         * 2. Convert each Transaction entity to TransactionResponse.
         *
         * @return List<TransactionResponse>
     */
    public List<TransactionResponse> getTransactionHistory(String accountNumber) {

        List<Transaction> transactions = transactionRepository
                .findBySenderAccountNumberOrReceiverAccountNumber(accountNumber);

        return transactions.stream().map(this::mapToResponse).toList();
    }

    /*
         * SAGA STEP 3 - Verify OTP
         *
         * @param transactionId
         *
         * @param otp
         *
         * PURPOSE:
         * - Verify the OTP generated for the transaction.
         *
         * FLOW:
         * 1. Retrieve transaction from database.
         * 2. Get OTP from Redis using transactionId.
         *
         * OTP EXPIRED:
         * - Compensate transaction.
         * - Refund amount to sender.
         *
         * WRONG OTP:
         * - Delete OTP from Redis.
         * - Publish fraud.detected event.
         * - Block sender account.
         * - Compensate transaction.
         * - Refund amount.
         *
         * CORRECT OTP:
         * - Delete OTP from Redis.
         * - Complete the transaction.
         *
         * @return TransactionResponse
     */
    public TransactionResponse verifyOTP(String transactionId, String otp) {

        log.info("OTP Verification for the transaction : {} ", transactionId);
        Transaction transaction = transactionRepository.findById(transactionId)
                .orElseThrow(() -> new RuntimeException("Transaction Not Found " + transactionId));

        if (transaction.getTransactionStatus() != TransactionStatus.PENDING_VERIFICATION) {
            throw new RuntimeException(
                    "Transaction is not waiting for OTP verification: " + transactionId
            );
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
         * SAGA COMPLETION - completeTransactionResponse
         *
         * PURPOSE:
         * - Complete the transaction after all required
         * verification steps have succeeded.
         *
         * FLOW:
         * 1. Credit the receiver account.
         * 2. Change transaction status to COMPLETED.
         * 3. Set completedAt timestamp.
         * 4. Save updated transaction.
         * 5. Publish transaction.completed event.
         *
         * SUCCESS:
         * - Receiver receives the transferred amount.
         * - Transaction becomes COMPLETED.
         *
         * @param transaction
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
         * SAGA COMPENSATION - Wrong OTP
         *
         * PURPOSE:
         * - Handle a security failure caused by an incorrect OTP.
         *
         * FLOW:
         * 1. Publish fraud.detected event.
         * 2. Account Service consumes the event and blocks the account.
         * 3. Execute Saga compensation.
         * 4. Refund the transferred amount to the sender.
         * 5. Mark the transaction as FLAGGED.
         *
         * RESULT:
         * - Account is blocked.
         * - Transaction is cancelled/flagged.
         * - Amount is refunded.
         *
         * @param transaction
         *
         * @param reason
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
         * SAGA COMPENSATION - Refund Transaction
         *
         * PURPOSE:
         * - Roll back the previous successful Saga operation
         * when the transaction cannot be completed.
         *
         * FLOW:
         * 1. Credit the amount back to the sender.
         * 2. Mark transaction as FLAGGED.
         * 3. Store the failure reason.
         * 4. Save transaction.
         * 5. Publish transaction.refunded event.
         *
         * RESULT:
         * - Sender receives the refunded amount.
         * - Transaction is marked as FLAGGED.
         * - Notification Service can notify the user.
         *
         * @param transaction
         *
         * @param reason
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
        refundEvent.put("sernderAccountNumber", transaction.getSenderAccountNumber());
        refundEvent.put("reason", reason);

        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC, transaction.getId(), refundEvent);
        log.info("SAGA COMPENSATION COMPLETE - {} refunded to {} ", transaction.getAmount(),
                transaction.getSenderAccountNumber());

    }

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
         * SAGA CONTINUATION - Process Clean Fraud Result
         *
         * PURPOSE:
         * - Continue the Saga after Fraud Detection confirms
         * that the transaction is clean.
         *
         * FLOW:
         * 1. Retrieve transaction from database.
         * 2. Verify that transaction is still PROCESSING.
         * 3. If not PROCESSING -> skip processing.
         * 4. Complete the transaction.
         *
         * SAFETY CHECK:
         * - Prevent completing a transaction that has already
         * been cancelled, refunded, or completed.
         *
         * @param transactionId
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
