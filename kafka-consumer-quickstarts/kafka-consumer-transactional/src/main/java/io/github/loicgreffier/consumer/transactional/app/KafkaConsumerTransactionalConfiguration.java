package io.github.loicgreffier.consumer.transactional.app;

import io.github.loicgreffier.consumer.transactional.properties.KafkaConsumerProperties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class KafkaConsumerTransactionalConfiguration {
    @Bean
    @Scope("prototype")
    public Consumer<String, String> kafkaConsumer(KafkaConsumerProperties properties) {
        return new KafkaConsumer<>(properties.asProperties());
    }
}
