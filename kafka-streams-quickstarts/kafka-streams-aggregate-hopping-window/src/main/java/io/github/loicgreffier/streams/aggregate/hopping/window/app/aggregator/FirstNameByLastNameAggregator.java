package io.github.loicgreffier.streams.aggregate.hopping.window.app.aggregator;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonGroup;
import org.apache.kafka.streams.kstream.Aggregator;

import java.util.ArrayList;
import java.util.List;

public class FirstNameByLastNameAggregator implements Aggregator<String, KafkaPerson, KafkaPersonGroup> {
    @Override
    public KafkaPersonGroup apply(String key, KafkaPerson kafkaPerson, KafkaPersonGroup aggregate) {
        aggregate.getFirstNameByLastName().putIfAbsent(kafkaPerson.getLastName(), new ArrayList<>());

        List<String> firstNames = aggregate.getFirstNameByLastName().get(kafkaPerson.getLastName());
        firstNames.add(kafkaPerson.getFirstName());
        aggregate.getFirstNameByLastName().put(kafkaPerson.getLastName(), firstNames);

        return aggregate;
    }
}
