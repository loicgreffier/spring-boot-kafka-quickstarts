package io.github.loicgreffier.consumer.simple;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaConsumerSimpleApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerSimpleApplication.class, args);
    }
}
