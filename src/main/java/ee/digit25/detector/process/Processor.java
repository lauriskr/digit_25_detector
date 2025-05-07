package ee.digit25.detector.process;

import ee.digit25.detector.domain.transaction.TransactionValidator;
import ee.digit25.detector.domain.transaction.external.TransactionRequester;
import ee.digit25.detector.domain.transaction.external.TransactionVerifier;
import ee.digit25.detector.domain.transaction.external.api.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Service
@RequiredArgsConstructor
public class Processor {

    private final int TRANSACTION_BATCH_SIZE = 50;
    private final TransactionRequester requester;
    private final TransactionValidator validator;
    private final TransactionVerifier verifier;
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);

    @Scheduled(fixedDelay = 100)
    public void process() {
        log.info("Starting to process a batch of transactions of size {}", TRANSACTION_BATCH_SIZE);

        List<Transaction> transactions = requester.getUnverified(TRANSACTION_BATCH_SIZE);
        
        if (transactions.isEmpty()) {
            return;
        }

        List<Transaction> legitimateTransactions = new ArrayList<>();
        List<Transaction> fraudulentTransactions = new ArrayList<>();

        // Process validation in parallel
        List<CompletableFuture<Void>> futures = transactions.stream()
            .map(transaction -> CompletableFuture.runAsync(() -> {
                if (validator.isLegitimate(transaction)) {
                    synchronized (legitimateTransactions) {
                        legitimateTransactions.add(transaction);
                    }
                } else {
                    synchronized (fraudulentTransactions) {
                        fraudulentTransactions.add(transaction);
                    }
                }
            }, executorService))
            .toList();

        // Wait for all validation tasks to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        // Batch verify/reject
        if (!legitimateTransactions.isEmpty()) {
            verifier.verify(legitimateTransactions);
            log.info("Verified {} legitimate transactions", legitimateTransactions.size());
        }
        
        if (!fraudulentTransactions.isEmpty()) {
            verifier.reject(fraudulentTransactions);
            log.info("Rejected {} fraudulent transactions", fraudulentTransactions.size());
        }
    }
}
