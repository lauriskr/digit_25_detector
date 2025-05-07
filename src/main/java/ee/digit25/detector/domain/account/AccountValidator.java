package ee.digit25.detector.domain.account;

import ee.digit25.detector.domain.account.external.AccountRequester;
import ee.digit25.detector.domain.account.external.api.Account;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Slf4j
@Service
@RequiredArgsConstructor
public class AccountValidator {

    private final AccountRequester requester;

    public boolean isValidSenderAccount(String accountNumber, BigDecimal amount, String senderPersonCode) {
        log.info("Checking if account {} is valid sender account", accountNumber);
        
        Account account = requester.get(accountNumber);
        
        return !account.getClosed() 
            && senderPersonCode.equals(account.getOwner()) 
            && account.getBalance().compareTo(amount) >= 0;
    }

    public boolean isValidRecipientAccount(String accountNumber, String recipientPersonCode) {
        log.info("Checking if account {} is valid recipient account", accountNumber);
        
        Account account = requester.get(accountNumber);
        
        return !account.getClosed() && recipientPersonCode.equals(account.getOwner());
    }
}
