package io.github.loicgreffier.producer.headers.config;

import io.github.loicgreffier.producer.headers.property.ProducerProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * This class provides configuration for creating a Kafka producer instance.
 */
@Configuration
public class ProducerConfig {
    /**
     * Creates a Kafka producer instance using the specified properties.
     * When the application is stopped, the producer bean is automatically destroyed
     * and the {@link Producer#close()} method is automatically called.
     *
     * @param properties The producer properties to configure the Kafka producer.
     * @return A Kafka producer instance.
     */
    @Bean
    public Producer<String, String> kafkaProducer(ProducerProperties properties) {
        return new KafkaProducer<>(properties.asProperties());
    }
}
