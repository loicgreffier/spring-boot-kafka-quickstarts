package io.github.loicgreffier.streams.aggregate;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaStreamsAggregateApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsAggregateApplication.class, args);
    }
}
