package io.github.loicgreffier.streams.average.app.aggregator;

import io.github.loicgreffier.avro.KafkaAverageAge;
import io.github.loicgreffier.avro.KafkaPerson;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneOffset;
import org.apache.kafka.streams.kstream.Aggregator;

/**
 * This class represents an aggregator that aggregates the age.
 */
public class AgeAggregator implements Aggregator<String, KafkaPerson, KafkaAverageAge> {
    /**
     * Aggregates the age.
     *
     * @param key         The key of the record.
     * @param kafkaPerson The value of the record.
     * @param aggregate   The aggregate.
     * @return The updated aggregate.
     */
    @Override
    public KafkaAverageAge apply(String key, KafkaPerson kafkaPerson, KafkaAverageAge aggregate) {
        aggregate.setCount(aggregate.getCount() + 1);

        LocalDate currentDate = LocalDate.now();
        LocalDate birthDate = LocalDate.ofInstant(kafkaPerson.getBirthDate(), ZoneOffset.UTC);
        aggregate.setAgeSum(
            aggregate.getAgeSum() + Period.between(birthDate, currentDate).getYears());
        return aggregate;
    }
}
