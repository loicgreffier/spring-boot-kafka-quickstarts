package io.github.loicgreffier.streams.average;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaStreamsAverageApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsAverageApplication.class, args);
    }
}
