package io.github.loicgreffier.streams.producer.country.app;

import io.github.loicgreffier.streams.producer.country.properties.KafkaProducerProperties;
import io.github.loicgreffier.avro.KafkaCountry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaStreamsProducerCountryConfiguration {
    @Bean
    public Producer<String, KafkaCountry> kafkaProducer(KafkaProducerProperties properties) {
        return new KafkaProducer<>(properties.asProperties());
    }
}
