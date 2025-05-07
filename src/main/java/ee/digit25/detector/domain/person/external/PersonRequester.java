package ee.digit25.detector.domain.person.external;

import ee.digit25.detector.domain.person.external.api.Person;
import ee.digit25.detector.domain.person.external.api.PersonApi;
import ee.digit25.detector.domain.person.external.api.PersonApiProperties;
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
public class PersonRequester {

    private final PersonApi api;
    private final PersonApiProperties properties;
    private static final int BATCH_SIZE = 25;

    public Person getPerson(String personCode) {
        try {
            log.info("Requesting person: {}", personCode);
            Response<Person> response = api.get(properties.getToken(), personCode).execute();
            
            if (!response.isSuccessful()) {
                log.error("Failed to get person: {} - {}", personCode, response.code());
                return null;
            }
            
            return response.body();
        } catch (IOException e) {
            log.error("Error getting person: {}", personCode, e);
            return null;
        }
    }

    public Map<String, Person> getPersons(List<String> personCodes) {
        if (personCodes == null || personCodes.isEmpty()) {
            return Collections.emptyMap();
        }

        // Remove duplicates
        List<String> uniqueCodes = new ArrayList<>(new HashSet<>(personCodes));
        
        // Split into batches
        List<List<String>> batches = new ArrayList<>();
        for (int i = 0; i < uniqueCodes.size(); i += BATCH_SIZE) {
            batches.add(uniqueCodes.subList(i, Math.min(i + BATCH_SIZE, uniqueCodes.size())));
        }

        Map<String, Person> result = new HashMap<>();
        
        for (List<String> batch : batches) {
            try {
                log.info("Requesting batch of {} persons", batch.size());
                Response<List<Person>> response = api.get(properties.getToken(), batch).execute();
                
                if (!response.isSuccessful()) {
                    log.error("Failed to get persons batch - {}", response.code());
                    continue;
                }
                
                List<Person> persons = response.body();
                if (persons != null) {
                    persons.forEach(person -> result.put(person.getPersonCode(), person));
                }
            } catch (IOException e) {
                log.error("Error getting persons batch", e);
            }
        }
        
        return result;
    }

    public CompletableFuture<Map<String, Person>> getPersonsAsync(List<String> personCodes) {
        return CompletableFuture.supplyAsync(() -> getPersons(personCodes))
            .orTimeout(5000, TimeUnit.MILLISECONDS)
            .exceptionally(ex -> {
                log.error("Error in async person batch request", ex);
                return Collections.emptyMap();
            });
    }
}
