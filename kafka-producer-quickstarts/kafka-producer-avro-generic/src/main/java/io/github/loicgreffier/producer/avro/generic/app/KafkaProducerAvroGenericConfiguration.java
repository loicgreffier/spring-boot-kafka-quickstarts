package io.github.loicgreffier.producer.avro.generic.app;

import io.github.loicgreffier.producer.avro.generic.properties.ProducerProperties;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaProducerAvroGenericConfiguration {
    @Bean
    public Producer<String, GenericRecord> kafkaProducerBean(ProducerProperties producerProperties) {
        return new KafkaProducer<>(producerProperties.asProperties());
    }
}
