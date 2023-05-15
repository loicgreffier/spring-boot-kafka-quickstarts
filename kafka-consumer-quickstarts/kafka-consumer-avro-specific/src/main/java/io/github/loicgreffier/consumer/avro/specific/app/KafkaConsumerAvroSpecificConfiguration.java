package io.github.loicgreffier.consumer.avro.specific.app;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.consumer.avro.specific.properties.ConsumerProperties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class KafkaConsumerAvroSpecificConfiguration {
    @Bean
    @Scope("prototype")
    public Consumer<String, KafkaPerson> kafkaConsumerBean(ConsumerProperties consumerProperties) {
        return new KafkaConsumer<>(consumerProperties.asProperties());
    }
}
