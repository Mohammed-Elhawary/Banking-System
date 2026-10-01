package com.banking.account_service.exception.handler;

import java.time.LocalDateTime;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import com.banking.account_service.exception.account.AccountAlreadyExistsException;
import com.banking.account_service.exception.account.AccountNotFoundException;
import com.banking.account_service.exception.account.AccountOperationNotAllowedException;
import com.banking.account_service.exception.balance.InsufficientBalanceException;
import com.banking.account_service.exception.balance.InvalidAmountException;
import com.banking.account_service.exception.dto.ErrorResponse;

import jakarta.servlet.http.HttpServletRequest;

@RestControllerAdvice
public class GlobalExceptionHandler {

        @ExceptionHandler(AccountNotFoundException.class)
        public ResponseEntity<ErrorResponse> handlAccountNotFoundException(AccountNotFoundException ex,
                        HttpServletRequest request) {

                ErrorResponse errorResponse = ErrorResponse.builder()
                                .message(ex.getMessage())
                                .path(request.getRequestURI())
                                .statusCode(HttpStatus.NOT_FOUND.value())
                                .status(HttpStatus.NOT_FOUND.getReasonPhrase())
                                .timestamp(LocalDateTime.now())
                                .build();

                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(errorResponse);
        }

        @ExceptionHandler(AccountAlreadyExistsException.class)
        public ResponseEntity<ErrorResponse> handlAccountAlreadyExsistException(AccountAlreadyExistsException ex,
                        HttpServletRequest request) {

                ErrorResponse errorResponse = ErrorResponse.builder()
                                .message(ex.getMessage())
                                .path(request.getRequestURI())
                                .statusCode(HttpStatus.CONFLICT.value())
                                .status(HttpStatus.CONFLICT.getReasonPhrase())
                                .timestamp(LocalDateTime.now())
                                .build();

                return ResponseEntity.status(HttpStatus.CONFLICT).body(errorResponse);

        }

        @ExceptionHandler(AccountOperationNotAllowedException.class)
        public ResponseEntity<ErrorResponse> handlAccountOperationNotAllowedException(
                        AccountOperationNotAllowedException ex,
                        HttpServletRequest request) {

                ErrorResponse errorResponse = ErrorResponse.builder()
                                .message(ex.getMessage())
                                .path(request.getRequestURI())
                                .statusCode(HttpStatus.FORBIDDEN.value())
                                .status(HttpStatus.FORBIDDEN.getReasonPhrase())
                                .timestamp(LocalDateTime.now())
                                .build();

                return ResponseEntity.status(HttpStatus.FORBIDDEN).body(errorResponse);
        }

        @ExceptionHandler(InvalidAmountException.class)
        public ResponseEntity<ErrorResponse> handlInvalidAmountException(
                        InvalidAmountException ex,
                        HttpServletRequest request) {

                ErrorResponse errorResponse = ErrorResponse.builder()
                                .message(ex.getMessage())
                                .path(request.getRequestURI())
                                .statusCode(HttpStatus.BAD_REQUEST.value())
                                .status(HttpStatus.BAD_REQUEST.getReasonPhrase())
                                .timestamp(LocalDateTime.now())
                                .build();

                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
        }

        @ExceptionHandler(InsufficientBalanceException.class)
        public ResponseEntity<ErrorResponse> handlInsufficientBalanceException(
                        InsufficientBalanceException ex,
                        HttpServletRequest request) {

                ErrorResponse errorResponse = ErrorResponse.builder()
                                .message(ex.getMessage())
                                .path(request.getRequestURI())
                                .statusCode(HttpStatus.UNPROCESSABLE_CONTENT.value())
                                .status(HttpStatus.UNPROCESSABLE_CONTENT.getReasonPhrase())
                                .timestamp(LocalDateTime.now())
                                .build();

                return ResponseEntity.status(HttpStatus.UNPROCESSABLE_CONTENT).body(errorResponse);
        }
}
