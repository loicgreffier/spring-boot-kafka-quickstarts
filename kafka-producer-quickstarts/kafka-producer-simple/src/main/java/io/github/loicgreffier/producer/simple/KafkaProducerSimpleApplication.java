package io.github.loicgreffier.producer.simple;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@SpringBootApplication
public class KafkaProducerSimpleApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaProducerSimpleApplication.class, args);
    }
}
