package io.github.loicgreffier.consumer.avro.specific.config;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.consumer.avro.specific.properties.KafkaConsumerProperties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConsumerConfig {
    @Bean(destroyMethod = "wakeup")
    public Consumer<String, KafkaPerson> kafkaConsumer(KafkaConsumerProperties properties) {
        return new KafkaConsumer<>(properties.asProperties());
    }
}
