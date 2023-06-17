package io.github.loicgreffier.streams.schedule.store.cleanup;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaStreamsScheduleStoreCleanupApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsScheduleStoreCleanupApplication.class, args);
    }
}
