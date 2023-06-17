package io.github.loicgreffier.consumer.circuit.breaker.app;

import io.github.loicgreffier.consumer.circuit.breaker.properties.KafkaConsumerProperties;
import io.github.loicgreffier.avro.KafkaPerson;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class KafkaConsumerCircuitBreakerConfiguration {
    @Bean
    @Scope("prototype")
    public Consumer<String, KafkaPerson> kafkaConsumer(KafkaConsumerProperties properties) {
        return new KafkaConsumer<>(properties.asProperties());
    }
}
