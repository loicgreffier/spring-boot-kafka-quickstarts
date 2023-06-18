package io.github.loicgreffier.streams.average.app.aggregator;

import io.github.loicgreffier.avro.KafkaAverageAge;
import io.github.loicgreffier.avro.KafkaPerson;
import org.apache.kafka.streams.kstream.Aggregator;

import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneOffset;

public class AgeAggregator implements Aggregator<String, KafkaPerson, KafkaAverageAge> {
    @Override
    public KafkaAverageAge apply(String key, KafkaPerson value, KafkaAverageAge aggregate) {
        aggregate.setCount(aggregate.getCount() + 1);

        LocalDate currentDate = LocalDate.now();
        LocalDate birthDate = LocalDate.ofInstant(value.getBirthDate(), ZoneOffset.UTC);
        aggregate.setAgeSum(aggregate.getAgeSum() + Period.between(birthDate, currentDate).getYears());
        return aggregate;
    }
}
