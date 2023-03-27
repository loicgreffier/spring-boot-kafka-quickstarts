package io.github.loicgreffier.streams.producer.country.app;

import io.github.loicgreffier.streams.producer.country.properties.ProducerProperties;
import io.github.loicgreffier.avro.KafkaCountry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaStreamsProducerCountryConfiguration {
    @Bean
    public Producer<String, KafkaCountry> kafkaProducerBean(ProducerProperties producerProperties) {
        return new KafkaProducer<>(producerProperties.asProperties());
    }
}
