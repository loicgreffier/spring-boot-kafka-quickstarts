package io.github.loicgreffier.streams.aggregate.hopping.window;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonGroup;
import io.github.loicgreffier.streams.aggregate.hopping.window.app.aggregator.FirstNameByLastNameAggregator;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

class FirstNameByLastNameAggregatorTests {
    @Test
    void shouldAggregateFirstNamesByLastName() {
        FirstNameByLastNameAggregator firstNameByLastNameAggregator = new FirstNameByLastNameAggregator();
        KafkaPersonGroup kafkaPersonGroup = new KafkaPersonGroup(new HashMap<>());

        firstNameByLastNameAggregator.apply("Allen", KafkaPerson.newBuilder()
                .setId(1L)
                .setFirstName("Aaran")
                .setLastName("Allen")
                .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
                .build(), kafkaPersonGroup);

        firstNameByLastNameAggregator.apply("Allen", KafkaPerson.newBuilder()
                .setId(2L)
                .setFirstName("Brendan")
                .setLastName("Allen")
                .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
                .build(), kafkaPersonGroup);

        firstNameByLastNameAggregator.apply("Wise", KafkaPerson.newBuilder()
                .setId(3L)
                .setFirstName("Bret")
                .setLastName("Wise")
                .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
                .build(), kafkaPersonGroup);

        assertThat(kafkaPersonGroup.getFirstNameByLastName()).hasSize(2);

        assertThat(kafkaPersonGroup.getFirstNameByLastName()).containsKey("Allen");
        assertThat(kafkaPersonGroup.getFirstNameByLastName().get("Allen")).hasSize(2);
        assertThat(kafkaPersonGroup.getFirstNameByLastName().get("Allen").get(0)).isEqualTo("Aaran");
        assertThat(kafkaPersonGroup.getFirstNameByLastName().get("Allen").get(1)).isEqualTo("Brendan");

        assertThat(kafkaPersonGroup.getFirstNameByLastName()).containsKey("Wise");
        assertThat(kafkaPersonGroup.getFirstNameByLastName().get("Wise")).hasSize(1);
        assertThat(kafkaPersonGroup.getFirstNameByLastName().get("Wise").get(0)).isEqualTo("Bret");
    }
}
