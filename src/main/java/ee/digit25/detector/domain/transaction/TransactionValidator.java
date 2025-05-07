package ee.digit25.detector.domain.transaction;

import ee.digit25.detector.domain.account.AccountValidator;
import ee.digit25.detector.domain.device.DeviceValidator;
import ee.digit25.detector.domain.person.PersonValidator;
import ee.digit25.detector.domain.transaction.external.api.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionValidator {

    private final PersonValidator personValidator;
    private final DeviceValidator deviceValidator;
    private final AccountValidator accountValidator;

    public boolean isLegitimate(Transaction transaction) {
        try {
            return isLegitimateAsync(transaction).get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Error validating transaction", e);
            return false;
        }
    }

    public CompletableFuture<Boolean> isLegitimateAsync(Transaction transaction) {
        // Start all validations in parallel
        CompletableFuture<Map<String, Boolean>> personValidations = personValidator.areValidAsync(
            List.of(transaction.getRecipient(), transaction.getSender())
        );

        CompletableFuture<Map<String, Boolean>> deviceValidations = deviceValidator.areValidAsync(
            List.of(transaction.getDeviceMac())
        );

        CompletableFuture<Map<String, Boolean>> senderAccountValidations = accountValidator.areValidSenderAccountsAsync(
            List.of(transaction.getSenderAccount()),
            transaction.getAmount(),
            transaction.getSender()
        );

        CompletableFuture<Map<String, Boolean>> recipientAccountValidations = accountValidator.areValidRecipientAccountsAsync(
            List.of(transaction.getRecipientAccount()),
            transaction.getRecipient()
        );

        // Combine all results
        return CompletableFuture.allOf(
            personValidations,
            deviceValidations,
            senderAccountValidations,
            recipientAccountValidations
        ).thenApply(v -> {
            boolean isLegitimate = true;

            // Check person validations
            Map<String, Boolean> personResults = personValidations.join();
            isLegitimate &= personResults.getOrDefault(transaction.getRecipient(), false);
            isLegitimate &= personResults.getOrDefault(transaction.getSender(), false);

            // Check device validation
            Map<String, Boolean> deviceResults = deviceValidations.join();
            isLegitimate &= deviceResults.getOrDefault(transaction.getDeviceMac(), false);

            // Check account validations
            Map<String, Boolean> senderAccountResults = senderAccountValidations.join();
            isLegitimate &= senderAccountResults.getOrDefault(transaction.getSenderAccount(), false);

            Map<String, Boolean> recipientAccountResults = recipientAccountValidations.join();
            isLegitimate &= recipientAccountResults.getOrDefault(transaction.getRecipientAccount(), false);

            return isLegitimate;
        }).exceptionally(ex -> {
            log.error("Error in async transaction validation", ex);
            return false;
        });
    }
}
