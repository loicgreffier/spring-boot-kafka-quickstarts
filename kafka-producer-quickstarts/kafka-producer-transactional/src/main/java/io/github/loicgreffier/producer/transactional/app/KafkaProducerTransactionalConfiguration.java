package io.github.loicgreffier.producer.transactional.app;

import io.github.loicgreffier.producer.transactional.properties.KafkaProducerProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaProducerTransactionalConfiguration {
    @Bean
    public Producer<String, String> kafkaProducer(KafkaProducerProperties properties) {
        return new KafkaProducer<>(properties.asProperties());
    }
}
