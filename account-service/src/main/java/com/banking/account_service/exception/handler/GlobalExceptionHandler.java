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

/*
         * HANDLE ACCOUNT NOT FOUND
         *
         * PURPOSE:
         * - Translate AccountNotFoundException into a 404 response.
         *
         * FLOW:
         * 1. Build an ErrorResponse from the exception message and the
         * request URI.
         * 2. Stamp it with status 404 and the current time.
         * 3. Return it.
         *
         * @param ex      The exception that was raised.
         * @param request Request whose URI is echoed back.
         *
         * @return 404 with the error body.
         */
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

        /*
         * HANDLE ACCOUNT ALREADY EXISTS
         *
         * PURPOSE:
         * - Translate AccountAlreadyExistsException into a 409 response.
         *
         * FLOW:
         * 1. Build an ErrorResponse from the exception message and the
         * request URI.
         * 2. Stamp it with status 409 and the current time.
         * 3. Return it.
         *
         * @param ex      The exception that was raised.
         * @param request Request whose URI is echoed back.
         *
         * @return 409 with the error body.
         */
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

        /*
         * HANDLE ACCOUNT OPERATION NOT ALLOWED
         *
         * PURPOSE:
         * - Translate AccountOperationNotAllowedException into a 403.
         *
         * FLOW:
         * 1. Build an ErrorResponse from the exception message and the
         * request URI.
         * 2. Stamp it with status 403 and the current time.
         * 3. Return it.
         *
         * NOTE:
         * - This is the status a refund receives when a fraud block has
         * already frozen the account.
         *
         * @param ex      The exception that was raised.
         * @param request Request whose URI is echoed back.
         *
         * @return 403 with the error body.
         */
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

        /*
         * HANDLE INVALID AMOUNT
         *
         * PURPOSE:
         * - Translate InvalidAmountException into a 400 response.
         *
         * FLOW:
         * 1. Build an ErrorResponse from the exception message and the
         * request URI.
         * 2. Stamp it with status 400 and the current time.
         * 3. Return it.
         *
         * @param ex      The exception that was raised.
         * @param request Request whose URI is echoed back.
         *
         * @return 400 with the error body.
         */
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

        /*
         * HANDLE INSUFFICIENT BALANCE
         *
         * PURPOSE:
         * - Translate InsufficientBalanceException into a 422 response.
         *
         * FLOW:
         * 1. Build an ErrorResponse from the exception message and the
         * request URI.
         * 2. Stamp it with status 422 and the current time.
         * 3. Return it.
         *
         * NOTE:
         * - transaction-service calls this service over Feign, which
         * turns any non-2xx into an exception and discards this body.
         * The original message does not reach the transfer endpoint's
         * caller.
         *
         * @param ex      The exception that was raised.
         * @param request Request whose URI is echoed back.
         *
         * @return 422 with the error body.
         */
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
