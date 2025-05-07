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
import java.util.concurrent.ConcurrentLinkedDeque;
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
    private final ExecutorService executorService = Executors.newFixedThreadPool(50);
    private final ExecutorService verifyService = Executors.newFixedThreadPool(2);

    @Scheduled(fixedDelay = 10)
    public void process() {
        log.info("Starting to process a batch of transactions of size {}", TRANSACTION_BATCH_SIZE);

        List<Transaction> transactions = requester.getUnverified(TRANSACTION_BATCH_SIZE);
        
        if (transactions.isEmpty()) {
            return;
        }

        ConcurrentLinkedDeque<String> legitimateTransactionsIds = new ConcurrentLinkedDeque<>();
        ConcurrentLinkedDeque<String> fraudulentTransactionIds = new ConcurrentLinkedDeque<>();
//        List<Transaction> legitimateTransactionIds = new ArrayList<>();
//        List<Transaction> fraudulentTransactionIds = new ArrayList<>();

        // Process validation in parallel
        List<CompletableFuture<Void>> futures = transactions.stream()
            .map(transaction -> CompletableFuture.runAsync(() -> {
                if (validator.isLegitimate(transaction)) {
                    legitimateTransactionsIds.add(transaction.getId());
                } else {
                    fraudulentTransactionIds.add(transaction.getId());
                }
            }, executorService))
            .toList();

        // Wait for all validation tasks to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        CompletableFuture verify = CompletableFuture.runAsync(() -> {
            if (!legitimateTransactionsIds.isEmpty()) {
                verifier.verify(legitimateTransactionsIds.stream().toList());
                log.info("Verified {} legitimate transactions", legitimateTransactionsIds.size());
            }
        }, verifyService);

        CompletableFuture reject = CompletableFuture.runAsync(() -> {
            if (!fraudulentTransactionIds.isEmpty()) {
                verifier.reject(fraudulentTransactionIds.stream().toList());
                log.info("Rejected {} fraudulent transactions", fraudulentTransactionIds.size());
            }
        }, verifyService);
        // Batch verify/reject

        CompletableFuture.allOf(verify, reject).join();

    }
}
