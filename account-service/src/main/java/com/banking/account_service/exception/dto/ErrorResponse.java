package com.banking.account_service.exception.dto;

import java.time.LocalDateTime;


import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
@JsonPropertyOrder({
    "path",
    "message",
    "status",
    "error",
    "timestamp"
})
public class ErrorResponse {

    private String path;
    private String message;
    private int statusCode;
    private String status;
    private LocalDateTime timestamp;
}
