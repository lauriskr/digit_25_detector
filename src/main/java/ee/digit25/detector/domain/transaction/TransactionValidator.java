package ee.digit25.detector.domain.transaction;

import ee.digit25.detector.domain.account.AccountValidator;
import ee.digit25.detector.domain.device.DeviceValidator;
import ee.digit25.detector.domain.person.PersonValidator;
import ee.digit25.detector.domain.transaction.external.api.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionValidator {

    private final PersonValidator personValidator;
    private final DeviceValidator deviceValidator;
    private final AccountValidator accountValidator;

    public boolean isLegitimate(Transaction transaction) {
        boolean isLegitimate = true;

//        isLegitimate &= personValidator.isValid(transaction.getRecipient());
//        isLegitimate &= personValidator.isValid(transaction.getSender());

        isLegitimate &= personValidator.areValid(List.of(transaction.getRecipient(), transaction.getSender()));

        isLegitimate &= deviceValidator.isValid(transaction.getDeviceMac());
        isLegitimate &= accountValidator.isValidSenderAccount(transaction.getSenderAccount(), transaction.getAmount(), transaction.getSender());
        isLegitimate &= accountValidator.isValidRecipientAccount(transaction.getRecipientAccount(), transaction.getRecipient());

        return isLegitimate;
    }
}
