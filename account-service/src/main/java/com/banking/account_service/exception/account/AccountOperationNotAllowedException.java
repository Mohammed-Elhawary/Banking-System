
package com.banking.account_service.exception.account;

/**
 * AccountOperationNotAllowedException
 */
public class AccountOperationNotAllowedException extends RuntimeException {

    public AccountOperationNotAllowedException(String message) {
        super(message);
    }

}
