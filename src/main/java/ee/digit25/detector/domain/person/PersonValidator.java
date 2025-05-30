package ee.digit25.detector.domain.person;

import ee.digit25.detector.domain.person.external.PersonRequester;
import ee.digit25.detector.domain.person.external.api.Person;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class PersonValidator {

    private final PersonRequester requester;

    public boolean isValid(String personCode) {
        log.info("Validating person {}", personCode);
        
        Person person = requester.get(personCode);
        
        return !person.getWarrantIssued() && person.getHasContract() && !person.getBlacklisted();
    }

    public boolean areValid(List<String> personCodes) {
        log.info("Validating persons {}", personCodes);

        List<Person> persons = requester.get(personCodes);
        return persons.stream().allMatch(person ->
                !person.getWarrantIssued() && person.getHasContract() && !person.getBlacklisted()
        );
    }
}
