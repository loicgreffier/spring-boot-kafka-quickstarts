package io.lgr.quickstarts.streams.aggregate.tumbling.window.app.aggregator;

import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.avro.KafkaPersonGroup;
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
