package ee.digit25.detector.domain.person;

import ee.digit25.detector.domain.person.external.PersonRequester;
import ee.digit25.detector.domain.person.external.api.Person;
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
public class PersonValidator {

    private final PersonRequester requester;

    public boolean isValid(String personCode) {
        log.info("Validating person {}", personCode);
        
        Person person = requester.getPerson(personCode);
        if (person == null) {
            log.error("Failed to validate person: {}", personCode);
            return false;
        }
        
        return !person.getWarrantIssued() && person.getHasContract() && !person.getBlacklisted();
    }

    public Map<String, Boolean> areValid(List<String> personCodes) {
        log.info("Validating {} persons", personCodes.size());

        Map<String, Person> persons = requester.getPersons(personCodes);
        return personCodes.stream()
            .collect(java.util.stream.Collectors.toMap(
                code -> code,
                code -> {
                    Person person = persons.get(code);
                    if (person == null) {
                        log.error("Failed to validate person: {}", code);
                        return false;
                    }
                    return !person.getWarrantIssued() && person.getHasContract() && !person.getBlacklisted();
                }
            ));
    }

    public CompletableFuture<Map<String, Boolean>> areValidAsync(List<String> personCodes) {
        return requester.getPersonsAsync(personCodes)
            .thenApply(persons -> personCodes.stream()
                .collect(java.util.stream.Collectors.toMap(
                    code -> code,
                    code -> {
                        Person person = persons.get(code);
                        if (person == null) {
                            log.error("Failed to validate person: {}", code);
                            return false;
                        }
                        return !person.getWarrantIssued() && person.getHasContract() && !person.getBlacklisted();
                    }
                )))
            .orTimeout(5000, TimeUnit.MILLISECONDS)
            .exceptionally(ex -> {
                log.error("Error in async person validation", ex);
                return personCodes.stream()
                    .collect(java.util.stream.Collectors.toMap(
                        code -> code,
                        code -> false
                    ));
            });
    }
}
