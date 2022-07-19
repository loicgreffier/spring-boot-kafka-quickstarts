package io.lgr.quickstarts.consumer.retry.app;

import io.lgr.quickstarts.consumer.retry.properties.ConsumerProperties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class KafkaConsumerRetryConfiguration {
    @Bean
    @Scope("prototype")
    public Consumer<String, String> kafkaConsumerBean(ConsumerProperties consumerProperties) {
        return new KafkaConsumer<>(consumerProperties.asProperties());
    }
}
