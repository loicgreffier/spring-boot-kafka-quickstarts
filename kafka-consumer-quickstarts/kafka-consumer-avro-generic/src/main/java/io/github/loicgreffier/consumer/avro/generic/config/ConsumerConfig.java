package io.github.loicgreffier.consumer.avro.generic.config;

import io.github.loicgreffier.consumer.avro.generic.properties.ConsumerProperties;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConsumerConfig {
    @Bean(destroyMethod = "wakeup")
    public Consumer<String, GenericRecord> kafkaConsumer(ConsumerProperties properties) {
        return new KafkaConsumer<>(properties.asProperties());
    }
}
