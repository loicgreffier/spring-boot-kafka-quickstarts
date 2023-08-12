package io.github.loicgreffier.consumer.simple;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

/**
 * This is the main class for the Kafka consumer application.
 */
@EnableAsync
@SpringBootApplication
public class KafkaConsumerSimpleApplication {
    /**
     * The main entry point of the Kafka consumer application.
     *
     * @param args The command line arguments.
     */
    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerSimpleApplication.class, args);
    }
}
