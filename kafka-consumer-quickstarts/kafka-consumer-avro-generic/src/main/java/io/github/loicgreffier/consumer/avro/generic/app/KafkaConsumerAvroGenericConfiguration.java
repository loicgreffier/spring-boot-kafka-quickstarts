package io.github.loicgreffier.consumer.avro.generic.app;

import io.github.loicgreffier.consumer.avro.generic.properties.ConsumerProperties;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class KafkaConsumerAvroGenericConfiguration {
    @Bean
    @Scope("prototype")
    public Consumer<String, GenericRecord> kafkaConsumerBean(ConsumerProperties consumerProperties) {
        return new KafkaConsumer<>(consumerProperties.asProperties());
    }
}
