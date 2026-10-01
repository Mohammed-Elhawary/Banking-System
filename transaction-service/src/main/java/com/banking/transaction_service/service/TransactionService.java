package com.banking.transaction_service.service;

import java.util.List;
import java.util.UUID;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.banking.transaction_service.client.AccountServiceClient;
import com.banking.transaction_service.dto.TransactionResponse;
import com.banking.transaction_service.dto.TransferRequest;
import com.banking.transaction_service.entity.Transaction;
import com.banking.transaction_service.entity.TransactionStatus;
import com.banking.transaction_service.entity.TransactionType;
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

    private static final String TRANSACTION_INTIATED_TOPIC = "transaction.initiated";

    private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";

    private static final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";

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

    public TransactionResponse getTransaction(String transferId) {

        return mapToResponse(transactionRepository.findById(transferId)
                .orElseThrow(() -> new RuntimeException("Transaction Not Found")));

    }

    public List<TransactionResponse> getTransactionHistory(String accountNumber) {

        List<Transaction> transactions = transactionRepository
                .findBySenderAccountNumberOrReceiverAccountNumber(accountNumber);

        return transactions.stream().map(this::mapToResponse).toList();
    }

    public TransactionResponse verifyOTP(String transactionId, String otp) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'verifyOTP'");
    }

}
