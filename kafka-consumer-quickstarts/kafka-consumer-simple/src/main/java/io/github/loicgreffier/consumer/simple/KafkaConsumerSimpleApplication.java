package io.github.loicgreffier.consumer.simple;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@SpringBootApplication
public class KafkaConsumerSimpleApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerSimpleApplication.class, args);
    }
}
