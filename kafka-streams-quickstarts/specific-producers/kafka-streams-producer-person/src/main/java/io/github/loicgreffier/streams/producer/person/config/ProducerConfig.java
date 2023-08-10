package io.github.loicgreffier.streams.producer.person.config;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.producer.person.properties.ProducerProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ProducerConfig {
    @Bean
    public Producer<String, KafkaPerson> kafkaProducer(ProducerProperties properties) {
        return new KafkaProducer<>(properties.asProperties());
    }
}
