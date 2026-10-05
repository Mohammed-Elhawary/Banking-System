package com.banking.transaction_service.client;

import java.math.BigDecimal;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

@FeignClient(name = "account-service", url = "${account.service.url}")
public interface AccountServiceClient {

    @PostMapping("/api/v1/accounts/{accountNumber}/deduct")
    String deductBalance(@PathVariable String accountNumber, @RequestParam BigDecimal amount);

    @PostMapping("/api/v1/accounts/{accountNumber}/credit")
    public ResponseEntity<String> creditBalance(@PathVariable String accountNumber, @RequestParam BigDecimal amount);

}
