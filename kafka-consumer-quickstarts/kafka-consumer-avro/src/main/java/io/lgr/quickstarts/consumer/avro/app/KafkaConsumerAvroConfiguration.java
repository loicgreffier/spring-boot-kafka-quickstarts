package io.lgr.quickstarts.consumer.avro.app;

import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.consumer.avro.properties.ConsumerProperties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConsumerAvroConfiguration {
    @Bean
    public Consumer<String, KafkaPerson> kafkaConsumerBean(ConsumerProperties consumerProperties) {
        return new KafkaConsumer<>(consumerProperties.asProperties());
    }
}
