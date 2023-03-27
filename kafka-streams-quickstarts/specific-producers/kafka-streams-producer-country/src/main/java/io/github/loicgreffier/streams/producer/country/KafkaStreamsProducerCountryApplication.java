package io.github.loicgreffier.streams.producer.country;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaStreamsProducerCountryApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsProducerCountryApplication.class, args);
    }
}
