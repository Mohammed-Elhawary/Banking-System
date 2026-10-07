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

    /*
     * GENERATE ACCOUNT NUMBER
     *
     * PURPOSE:
     * - Produce a unique account number for a new account.
     *
     * FLOW:
     * 1. Pick a prefix from the account type: SAV, CUR or FIX.
     * 2. Append 10 random uppercase alphanumeric characters.
     * 3. Repeat while the candidate already exists in the database.
     * 4. Return the first unused number.
     *
     * NOTE:
     * - The existence check is a separate query per attempt, so two
     * concurrent creations can race and produce the same number.
     *
     * @param accountType Account type driving the prefix choice.
     *
     * @return An account number that was not present in the database.
     */
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

    /*
     * GENERATE RANDOM PART
     *
     * PURPOSE:
     * - Build the random suffix of an account number.
     *
     * FLOW:
     * 1. Repeat 10 times.
     * 2. Pick a random index from the allowed character set.
     * 3. Append that character.
     * 4. Return the assembled string.
     *
     * @return 10 random uppercase alphanumeric characters.
     */
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
