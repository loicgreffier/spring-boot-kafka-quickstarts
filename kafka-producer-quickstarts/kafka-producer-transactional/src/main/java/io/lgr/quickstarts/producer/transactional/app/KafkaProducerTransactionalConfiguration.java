package io.lgr.quickstarts.producer.transactional.app;

import io.lgr.quickstarts.producer.transactional.properties.ProducerProperties;
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
