package com.banking.notification_service.service;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import com.banking.notification_service.dto.AccountResponse;

@FeignClient(name = "account-service")
public interface AccountServiceClient {

    @GetMapping("/api/v1/accounts/{accountNumber}")
    public AccountResponse getAccount(@PathVariable String accountNumber);

}
