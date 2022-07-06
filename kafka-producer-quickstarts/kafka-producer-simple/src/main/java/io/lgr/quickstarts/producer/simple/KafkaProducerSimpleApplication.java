package io.lgr.quickstarts.producer.simple;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaProducerSimpleApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaProducerSimpleApplication.class, args);
    }
}
