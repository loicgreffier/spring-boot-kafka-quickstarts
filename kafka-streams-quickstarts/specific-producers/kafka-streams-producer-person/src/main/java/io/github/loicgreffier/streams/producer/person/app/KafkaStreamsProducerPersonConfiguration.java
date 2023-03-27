package io.github.loicgreffier.streams.producer.person.app;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.producer.person.properties.ProducerProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaStreamsProducerPersonConfiguration {
    @Bean
    public Producer<String, KafkaPerson> kafkaProducerBean(ProducerProperties producerProperties) {
        return new KafkaProducer<>(producerProperties.asProperties());
    }
}
