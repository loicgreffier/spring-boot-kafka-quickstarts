package io.github.loicgreffier.streams.aggregate.hopping.window.app.aggregator;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonGroup;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.streams.kstream.Aggregator;

/**
 * This class represents an aggregator that aggregates the first names by last name.
 */
public class FirstNameByLastNameAggregator
    implements Aggregator<String, KafkaPerson, KafkaPersonGroup> {

    /**
     * Aggregates the first names by last name.
     *
     * @param key         The key of the record.
     * @param kafkaPerson The value of the record.
     * @param aggregate   The aggregate.
     * @return The updated aggregate.
     */
    @Override
    public KafkaPersonGroup apply(String key, KafkaPerson kafkaPerson, KafkaPersonGroup aggregate) {
        aggregate.getFirstNameByLastName()
            .putIfAbsent(kafkaPerson.getLastName(), new ArrayList<>());

        List<String> firstNames = aggregate.getFirstNameByLastName().get(kafkaPerson.getLastName());
        firstNames.add(kafkaPerson.getFirstName());
        aggregate.getFirstNameByLastName().put(kafkaPerson.getLastName(), firstNames);

        return aggregate;
    }
}
