package io.github.loicgreffier.streams.producer.person;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaStreamsProducerPersonApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsProducerPersonApplication.class, args);
    }
}
