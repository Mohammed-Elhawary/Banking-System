package com.banking.transaction_service.service;

import java.util.List;

import org.springframework.stereotype.Service;

import com.banking.transaction_service.dto.TransactionResponse;
import com.banking.transaction_service.dto.TransferRequest;

@Service
public class TransactionService {

    public TransactionResponse transfer(TransferRequest request) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'transfer'");
    }

    public TransactionResponse getTransaction(String transerId) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getTransaction'");
    }

    public List<TransactionResponse> getTransactionHistory(String accountNumber) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getTransactionHistory'");
    }

    public TransactionResponse verifyOTP(String transactionId, String otp) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'verifyOTP'");
    }

}
