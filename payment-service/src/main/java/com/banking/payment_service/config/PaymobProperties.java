package com.banking.payment_service.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "paymob")
public record PaymobProperties(
        String secretKey,
        String integrationId,
        String baseUrl
        ) {}
