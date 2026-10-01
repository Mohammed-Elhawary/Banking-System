package com.banking.transaction_service.service;


import org.springframework.kafka.core.KafkaTemplate;

public class TransactionEventService {

    KafkaTemplate<String, Object> kafka;
}
