package io.github.loicgreffier.streams.schedule;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaStreamsScheduleApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsScheduleApplication.class, args);
    }
}
