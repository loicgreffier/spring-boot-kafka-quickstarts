package io.github.loicgreffier.consumer.circuit.breaker.config;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.consumer.circuit.breaker.properties.ConsumerProperties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConsumerConfig {
    @Bean(destroyMethod = "wakeup")
    public Consumer<String, KafkaPerson> kafkaConsumer(ConsumerProperties properties) {
        return new KafkaConsumer<>(properties.asProperties());
    }
}
