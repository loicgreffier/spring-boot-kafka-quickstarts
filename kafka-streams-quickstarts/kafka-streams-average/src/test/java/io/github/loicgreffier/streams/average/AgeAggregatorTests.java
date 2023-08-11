package io.github.loicgreffier.streams.average;

import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaAverageAge;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.average.app.aggregator.AgeAggregator;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

class AgeAggregatorTests {
    @Test
    void shouldAggregateAgeByNationality() {
        AgeAggregator averageAgeAggregator = new AgeAggregator();
        KafkaAverageAge kafkaAverageAge = new KafkaAverageAge(0L, 0L);

        LocalDate currentDate = LocalDate.now();

        KafkaPerson personOne = KafkaPerson.newBuilder()
                .setId(1L)
                .setFirstName("Aaran")
                .setLastName("Abbott")
                .setNationality(CountryCode.FR)
                .setBirthDate(currentDate.minusYears(25).atStartOfDay().toInstant(ZoneOffset.UTC)) // 25 years old
                .build();
        averageAgeAggregator.apply(CountryCode.FR.toString(), personOne, kafkaAverageAge);

        KafkaPerson personTwo = KafkaPerson.newBuilder()
                .setId(2L)
                .setFirstName("Brendan")
                .setLastName("Abbott")
                .setNationality(CountryCode.FR)
                .setBirthDate(currentDate.minusYears(50).atStartOfDay().toInstant(ZoneOffset.UTC)) // 50 years old
                .build();
        averageAgeAggregator.apply(CountryCode.FR.toString(), personTwo, kafkaAverageAge);

        KafkaPerson personThree = KafkaPerson.newBuilder()
                .setId(3L)
                .setFirstName("Bret")
                .setLastName("Holman")
                .setNationality(CountryCode.FR)
                .setBirthDate(currentDate.minusYears(75).atStartOfDay().toInstant(ZoneOffset.UTC)) // 75 years old
                .build();
        averageAgeAggregator.apply(CountryCode.FR.toString(), personThree, kafkaAverageAge);

        assertThat(kafkaAverageAge.getCount()).isEqualTo(3L);
        assertThat(kafkaAverageAge.getAgeSum()).isEqualTo(150L);
    }
}
