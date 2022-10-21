package io.lgr.quickstarts.streams.producer.country.app;

import io.lgr.quickstarts.avro.KafkaCountry;
import io.lgr.quickstarts.streams.producer.country.properties.ProducerProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaStreamsProducerCountryConfiguration {
    @Bean
    public Producer<String, KafkaCountry> kafkaProducerBean(ProducerProperties producerProperties) {
        return new KafkaProducer<>(producerProperties.asProperties());
    }
}
