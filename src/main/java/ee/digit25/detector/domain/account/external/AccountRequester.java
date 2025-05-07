package ee.digit25.detector.domain.account.external;

import ee.digit25.detector.domain.account.external.api.Account;
import ee.digit25.detector.domain.account.external.api.AccountApi;
import ee.digit25.detector.domain.account.external.api.AccountApiProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import retrofit2.Response;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class AccountRequester {

    private final AccountApi api;
    private final AccountApiProperties properties;
    private static final int BATCH_SIZE = 25;

    public Account getAccount(String accountNumber) {
        try {
            log.info("Requesting account: {}", accountNumber);
            Response<Account> response = api.get(properties.getToken(), accountNumber).execute();
            
            if (!response.isSuccessful()) {
                log.error("Failed to get account: {} - {}", accountNumber, response.code());
                return null;
            }
            
            return response.body();
        } catch (IOException e) {
            log.error("Error getting account: {}", accountNumber, e);
            return null;
        }
    }

    public Map<String, Account> getAccounts(List<String> accountNumbers) {
        if (accountNumbers == null || accountNumbers.isEmpty()) {
            return Collections.emptyMap();
        }

        // Remove duplicates
        List<String> uniqueNumbers = new ArrayList<>(new HashSet<>(accountNumbers));
        
        // Split into batches
        List<List<String>> batches = new ArrayList<>();
        for (int i = 0; i < uniqueNumbers.size(); i += BATCH_SIZE) {
            batches.add(uniqueNumbers.subList(i, Math.min(i + BATCH_SIZE, uniqueNumbers.size())));
        }

        Map<String, Account> result = new HashMap<>();
        
        for (List<String> batch : batches) {
            try {
                log.info("Requesting batch of {} accounts", batch.size());
                Response<List<Account>> response = api.get(properties.getToken(), batch).execute();
                
                if (!response.isSuccessful()) {
                    log.error("Failed to get accounts batch - {}", response.code());
                    continue;
                }
                
                List<Account> accounts = response.body();
                if (accounts != null) {
                    accounts.forEach(account -> result.put(account.getNumber(), account));
                }
            } catch (IOException e) {
                log.error("Error getting accounts batch", e);
            }
        }
        
        return result;
    }

    public CompletableFuture<Map<String, Account>> getAccountsAsync(List<String> accountNumbers) {
        return CompletableFuture.supplyAsync(() -> getAccounts(accountNumbers))
            .orTimeout(5000, TimeUnit.MILLISECONDS)
            .exceptionally(ex -> {
                log.error("Error in async account batch request", ex);
                return Collections.emptyMap();
            });
    }
}
