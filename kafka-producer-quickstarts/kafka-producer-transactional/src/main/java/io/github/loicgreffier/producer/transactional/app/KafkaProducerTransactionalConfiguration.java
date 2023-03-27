package io.github.loicgreffier.producer.transactional.app;

import io.github.loicgreffier.producer.transactional.properties.ProducerProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaProducerTransactionalConfiguration {
    @Bean
    public Producer<String, String> kafkaProducerBean(ProducerProperties producerProperties) {
        return new KafkaProducer<>(producerProperties.asProperties());
    }
}
