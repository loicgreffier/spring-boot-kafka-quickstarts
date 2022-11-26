package io.lgr.quickstarts.streams.cogroup;

import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.avro.KafkaPersonGroup;
import io.lgr.quickstarts.streams.cogroup.app.aggregator.FirstNameByLastNameAggregator;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

class FirstNameByLastNameAggregatorTest {
    @Test
    void shouldAggregateFirstNamesByLastName() {
        FirstNameByLastNameAggregator firstNameByLastNameAggregator = new FirstNameByLastNameAggregator();
        KafkaPersonGroup kafkaPersonGroup = new KafkaPersonGroup(new HashMap<>());

        firstNameByLastNameAggregator.apply("Abbott", KafkaPerson.newBuilder()
                .setId(1L)
                .setFirstName("Aaran")
                .setLastName("Abbott")
                .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
                .build(), kafkaPersonGroup);

        firstNameByLastNameAggregator.apply("Abbott", KafkaPerson.newBuilder()
                .setId(2L)
                .setFirstName("Brendan")
                .setLastName("Abbott")
                .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
                .build(), kafkaPersonGroup);

        firstNameByLastNameAggregator.apply("Holman", KafkaPerson.newBuilder()
                .setId(3L)
                .setFirstName("Bret")
                .setLastName("Holman")
                .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
                .build(), kafkaPersonGroup);

        assertThat(kafkaPersonGroup.getFirstNameByLastName()).hasSize(2);

        assertThat(kafkaPersonGroup.getFirstNameByLastName()).containsKey("Abbott");
        assertThat(kafkaPersonGroup.getFirstNameByLastName().get("Abbott")).hasSize(2);
        assertThat(kafkaPersonGroup.getFirstNameByLastName().get("Abbott").get(0)).isEqualTo("Aaran");
        assertThat(kafkaPersonGroup.getFirstNameByLastName().get("Abbott").get(1)).isEqualTo("Brendan");

        assertThat(kafkaPersonGroup.getFirstNameByLastName()).containsKey("Holman");
        assertThat(kafkaPersonGroup.getFirstNameByLastName().get("Holman")).hasSize(1);
        assertThat(kafkaPersonGroup.getFirstNameByLastName().get("Holman").get(0)).isEqualTo("Bret");
    }
}
