package ee.digit25.detector.domain.transaction.external;

import ee.digit25.detector.domain.transaction.external.api.Transaction;
import ee.digit25.detector.domain.transaction.external.api.TransactionApiProperties;
import ee.digit25.detector.domain.transaction.external.api.TransactionsApi;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import retrofit2.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class TransactionRequester {

    private final TransactionsApi api;
    private final TransactionApiProperties properties;
    private static final int BATCH_SIZE = 25;

    public List<Transaction> getUnverified(int amount) {
        try {
            log.info("Requesting a batch of unverified transactions of size {}", amount);
            Response<List<Transaction>> response = api.getUnverified(properties.getToken(), amount).execute();
            
            if (!response.isSuccessful()) {
                log.error("Failed to get unverified transactions - {}", response.code());
                return Collections.emptyList();
            }
            
            return response.body();
        } catch (IOException e) {
            log.error("Error getting unverified transactions", e);
            return Collections.emptyList();
        }
    }

    public List<Transaction> getUnverifiedBatch(int amount) {
        if (amount <= 0) {
            return Collections.emptyList();
        }

        // Split into smaller batches
        List<List<Transaction>> batches = new ArrayList<>();
        int remaining = amount;
        
        while (remaining > 0) {
            int batchSize = Math.min(remaining, BATCH_SIZE);
            List<Transaction> batch = getUnverified(batchSize);
            
            if (batch.isEmpty()) {
                break;
            }
            
            batches.add(batch);
            remaining -= batch.size();
        }

        // Combine all batches
        return batches.stream()
            .flatMap(List::stream)
            .toList();
    }

    public CompletableFuture<List<Transaction>> getUnverifiedAsync(int amount) {
        return CompletableFuture.supplyAsync(() -> getUnverified(amount))
            .orTimeout(5000, TimeUnit.MILLISECONDS)
            .exceptionally(ex -> {
                log.error("Error in async unverified transactions request", ex);
                return Collections.emptyList();
            });
    }

    public CompletableFuture<List<Transaction>> getUnverifiedBatchAsync(int amount) {
        return CompletableFuture.supplyAsync(() -> getUnverifiedBatch(amount))
            .orTimeout(5000, TimeUnit.MILLISECONDS)
            .exceptionally(ex -> {
                log.error("Error in async unverified transactions batch request", ex);
                return Collections.emptyList();
            });
    }
}
