package io.github.loicgreffier.streams.selectkey;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaStreamsSelectKeyApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsSelectKeyApplication.class, args);
    }
}
