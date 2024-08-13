package io.github.loicgreffier.streams.average;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaAverageAge;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.average.app.aggregator.AgeAggregator;
import java.time.LocalDate;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class AgeAggregatorTest {
    @Test
    void shouldAggregateAgeByNationality() {
        AgeAggregator aggregator = new AgeAggregator();
        KafkaAverageAge averageAge = new KafkaAverageAge(0L, 0L);

        LocalDate currentDate = LocalDate.now();

        KafkaPerson personOne = KafkaPerson.newBuilder()
            .setId(1L)
            .setFirstName("Bart")
            .setLastName("Simpson")
            .setNationality(CountryCode.US)
            .setBirthDate(currentDate.minusYears(25).atStartOfDay().toInstant(ZoneOffset.UTC)) // 25 years old
            .build();
        aggregator.apply(CountryCode.US.toString(), personOne, averageAge);

        KafkaPerson personTwo = KafkaPerson.newBuilder()
            .setId(2L)
            .setFirstName("Homer")
            .setLastName("Simpson")
            .setNationality(CountryCode.US)
            .setBirthDate(currentDate.minusYears(50).atStartOfDay().toInstant(ZoneOffset.UTC)) // 50 years old
            .build();
        aggregator.apply(CountryCode.US.toString(), personTwo, averageAge);

        KafkaPerson personThree = KafkaPerson.newBuilder()
            .setId(3L)
            .setFirstName("Abraham")
            .setLastName("Simpson")
            .setNationality(CountryCode.US)
            .setBirthDate(currentDate.minusYears(75).atStartOfDay().toInstant(ZoneOffset.UTC)) // 75 years old
            .build();
        aggregator.apply(CountryCode.US.toString(), personThree, averageAge);

        assertEquals(3L, averageAge.getCount());
        assertEquals(150L, averageAge.getAgeSum());
    }
}
