package com.banking.account_service.helper;

import java.util.concurrent.ThreadLocalRandom;

import org.springframework.stereotype.Component;

import com.banking.account_service.entities.AccountType;
import com.banking.account_service.repository.AccountRepository;

@Component
public class AccountNumberGenerator {

    private static final String CHARACTERS =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    private final AccountRepository accountRepository;

    public AccountNumberGenerator(AccountRepository accountRepository) {
        this.accountRepository = accountRepository;
    }

    public  String generate(AccountType accountType) {

        String prefix = switch (accountType) {
            case SAVING -> "SAV";
            case CURRENT -> "CUR";
            case FIXED_DEPOSIT -> "FIX";
        };

        String accountNumber;

        do {
            accountNumber = prefix + "-" + generateRandomPart();
        } while (accountRepository.existsByAccountNumber(accountNumber));

        return accountNumber;
    }

    private String generateRandomPart() {

        StringBuilder result = new StringBuilder(10);

        for (int i = 0; i < 10; i++) {
            int index = ThreadLocalRandom.current()
                    .nextInt(CHARACTERS.length());

            result.append(CHARACTERS.charAt(index));
        }

        return result.toString();
    }
}
