package io.github.loicgreffier.consumer.retry.config;

import io.github.loicgreffier.consumer.retry.properties.ConsumerProperties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * This class provides configuration for creating a Kafka consumer instance.
 */
@Configuration
public class ConsumerConfig {
    /**
     * Creates a Kafka consumer instance using the specified properties.
     * When the application is stopped, the consumer bean is automatically destroyed.
     * When the consumer bean is destroyed, the {@link Consumer#wakeup()} method is called
     * instead of {@link Consumer#close()} to ensure thread safety.
     *
     * @param properties The consumer properties to configure the Kafka consumer.
     * @return A Kafka consumer instance.
     */
    @Bean(destroyMethod = "wakeup")
    public Consumer<String, String> kafkaConsumer(ConsumerProperties properties) {
        return new KafkaConsumer<>(properties.asProperties());
    }
}
