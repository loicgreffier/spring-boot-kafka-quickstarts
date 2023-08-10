package io.github.loicgreffier.streams.producer.country.config;

import io.github.loicgreffier.streams.producer.country.properties.ProducerProperties;
import io.github.loicgreffier.avro.KafkaCountry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ProducerConfig {
    @Bean
    public Producer<String, KafkaCountry> kafkaProducer(ProducerProperties properties) {
        return new KafkaProducer<>(properties.asProperties());
    }
}
