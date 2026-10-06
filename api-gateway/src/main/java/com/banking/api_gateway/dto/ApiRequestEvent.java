package com.banking.api_gateway.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ApiRequestEvent {

    String requestId;
    String method;
    String path;
    String service;
    int status;
    long durationMs;
    String timestamp;
}
