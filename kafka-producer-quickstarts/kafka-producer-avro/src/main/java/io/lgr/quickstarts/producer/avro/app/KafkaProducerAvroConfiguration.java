package io.lgr.quickstarts.producer.avro.app;

import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.producer.avro.properties.ProducerProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaProducerAvroConfiguration {
    @Bean
    public Producer<String, KafkaPerson> kafkaProducerBean(ProducerProperties producerProperties) {
        return new KafkaProducer<>(producerProperties.asProperties());
    }
}
