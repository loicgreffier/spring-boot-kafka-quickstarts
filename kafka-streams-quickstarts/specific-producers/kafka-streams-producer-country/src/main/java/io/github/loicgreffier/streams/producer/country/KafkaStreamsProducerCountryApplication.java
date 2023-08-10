package io.github.loicgreffier.streams.producer.country;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@SpringBootApplication
public class KafkaStreamsProducerCountryApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsProducerCountryApplication.class, args);
    }
}
