package ee.digit25.detector.domain.transaction.external;

import ee.bitweb.core.retrofit.RetrofitRequestExecutor;
import ee.digit25.detector.domain.transaction.external.api.Transaction;
import ee.digit25.detector.domain.transaction.external.api.TransactionApiProperties;
import ee.digit25.detector.domain.transaction.external.api.TransactionsApi;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionVerifier {

    private final TransactionsApi api;
    private final TransactionApiProperties properties;

    public void verify(Transaction transaction) {
        log.info("Verifying transaction {}", transaction.getId());

        RetrofitRequestExecutor.executeRaw(api.verify(properties.getToken(), transaction.getId()));
    }

    public void reject(Transaction transaction) {
        log.info("Rejecting transaction {}", transaction.getId());

        RetrofitRequestExecutor.executeRaw(api.reject(properties.getToken(), transaction.getId()));
    }

    public void verify(List<String> ids) {
        log.info("Verifying transactions {}", ids);

        RetrofitRequestExecutor.executeRaw(api.verify(properties.getToken(), ids));
    }

    public void reject(List<String> ids) {
        log.info("Rejecting transactions {}", ids);

        RetrofitRequestExecutor.executeRaw(api.reject(properties.getToken(), ids));
    }
}
