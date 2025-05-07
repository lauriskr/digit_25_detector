package ee.digit25.detector.domain.account;

import ee.digit25.detector.domain.account.external.AccountRequester;
import ee.digit25.detector.domain.account.external.api.Account;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class AccountValidator {

    private final AccountRequester requester;

    public boolean isValidSenderAccount(String accountNumber, BigDecimal amount, String senderPersonCode) {
        log.info("Checking if account {} is valid sender account", accountNumber);
        
        Account account = requester.getAccount(accountNumber);
        if (account == null) {
            log.error("Failed to validate sender account: {}", accountNumber);
            return false;
        }
        
        return !account.getClosed() 
            && senderPersonCode.equals(account.getOwner()) 
            && account.getBalance().compareTo(amount) >= 0;
    }

    public boolean isValidRecipientAccount(String accountNumber, String recipientPersonCode) {
        log.info("Checking if account {} is valid recipient account", accountNumber);
        
        Account account = requester.getAccount(accountNumber);
        if (account == null) {
            log.error("Failed to validate recipient account: {}", accountNumber);
            return false;
        }
        
        return !account.getClosed() && recipientPersonCode.equals(account.getOwner());
    }

    public Map<String, Boolean> areValidSenderAccounts(List<String> accountNumbers, BigDecimal amount, String senderPersonCode) {
        log.info("Validating {} sender accounts", accountNumbers.size());

        Map<String, Account> accounts = requester.getAccounts(accountNumbers);
        return accountNumbers.stream()
            .collect(java.util.stream.Collectors.toMap(
                number -> number,
                number -> {
                    Account account = accounts.get(number);
                    if (account == null) {
                        log.error("Failed to validate sender account: {}", number);
                        return false;
                    }
                    return !account.getClosed() 
                        && senderPersonCode.equals(account.getOwner()) 
                        && account.getBalance().compareTo(amount) >= 0;
                }
            ));
    }

    public Map<String, Boolean> areValidRecipientAccounts(List<String> accountNumbers, String recipientPersonCode) {
        log.info("Validating {} recipient accounts", accountNumbers.size());

        Map<String, Account> accounts = requester.getAccounts(accountNumbers);
        return accountNumbers.stream()
            .collect(java.util.stream.Collectors.toMap(
                number -> number,
                number -> {
                    Account account = accounts.get(number);
                    if (account == null) {
                        log.error("Failed to validate recipient account: {}", number);
                        return false;
                    }
                    return !account.getClosed() && recipientPersonCode.equals(account.getOwner());
                }
            ));
    }

    public CompletableFuture<Map<String, Boolean>> areValidSenderAccountsAsync(List<String> accountNumbers, BigDecimal amount, String senderPersonCode) {
        return requester.getAccountsAsync(accountNumbers)
            .thenApply(accounts -> accountNumbers.stream()
                .collect(java.util.stream.Collectors.toMap(
                    number -> number,
                    number -> {
                        Account account = accounts.get(number);
                        if (account == null) {
                            log.error("Failed to validate sender account: {}", number);
                            return false;
                        }
                        return !account.getClosed() 
                            && senderPersonCode.equals(account.getOwner()) 
                            && account.getBalance().compareTo(amount) >= 0;
                    }
                )))
            .orTimeout(5000, TimeUnit.MILLISECONDS)
            .exceptionally(ex -> {
                log.error("Error in async sender account validation", ex);
                return accountNumbers.stream()
                    .collect(java.util.stream.Collectors.toMap(
                        number -> number,
                        number -> false
                    ));
            });
    }

    public CompletableFuture<Map<String, Boolean>> areValidRecipientAccountsAsync(List<String> accountNumbers, String recipientPersonCode) {
        return requester.getAccountsAsync(accountNumbers)
            .thenApply(accounts -> accountNumbers.stream()
                .collect(java.util.stream.Collectors.toMap(
                    number -> number,
                    number -> {
                        Account account = accounts.get(number);
                        if (account == null) {
                            log.error("Failed to validate recipient account: {}", number);
                            return false;
                        }
                        return !account.getClosed() && recipientPersonCode.equals(account.getOwner());
                    }
                )))
            .orTimeout(5000, TimeUnit.MILLISECONDS)
            .exceptionally(ex -> {
                log.error("Error in async recipient account validation", ex);
                return accountNumbers.stream()
                    .collect(java.util.stream.Collectors.toMap(
                        number -> number,
                        number -> false
                    ));
            });
    }
}
