package io.github.loicgreffier.producer.simple.config;

import io.github.loicgreffier.producer.simple.properties.ProducerProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ProducerConfig {
    @Bean
    public Producer<String, String> kafkaProducer(ProducerProperties properties) {
        return new KafkaProducer<>(properties.asProperties());
    }
}
