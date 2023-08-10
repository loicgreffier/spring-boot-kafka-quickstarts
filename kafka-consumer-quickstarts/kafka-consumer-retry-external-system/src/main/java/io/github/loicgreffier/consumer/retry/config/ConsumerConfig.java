package io.github.loicgreffier.consumer.retry.config;

import io.github.loicgreffier.consumer.retry.properties.ConsumerProperties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConsumerConfig {
    @Bean(destroyMethod = "wakeup")
    public Consumer<String, String> kafkaConsumer(ConsumerProperties properties) {
        return new KafkaConsumer<>(properties.asProperties());
    }
}
