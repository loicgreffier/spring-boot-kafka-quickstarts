package io.github.loicgreffier.streams.process;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaStreamsProcessApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsProcessApplication.class, args);
    }
}
