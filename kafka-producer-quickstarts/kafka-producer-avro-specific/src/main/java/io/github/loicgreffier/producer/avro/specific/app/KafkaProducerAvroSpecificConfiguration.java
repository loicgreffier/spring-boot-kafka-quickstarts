package io.github.loicgreffier.producer.avro.specific.app;

import io.github.loicgreffier.producer.avro.specific.properties.ProducerProperties;
import io.github.loicgreffier.avro.KafkaPerson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaProducerAvroSpecificConfiguration {
    @Bean
    public Producer<String, KafkaPerson> kafkaProducerBean(ProducerProperties producerProperties) {
        return new KafkaProducer<>(producerProperties.asProperties());
    }
}
