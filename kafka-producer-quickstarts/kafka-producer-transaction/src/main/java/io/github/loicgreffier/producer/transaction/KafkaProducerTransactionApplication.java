package io.github.loicgreffier.producer.transaction;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

/**
 * This is the main class for the Kafka producer application.
 */
@EnableAsync
@SpringBootApplication
public class KafkaProducerTransactionApplication {
    /**
     * The main entry point of the Kafka producer application.
     *
     * @param args The command line arguments.
     */
    public static void main(String[] args) {
        SpringApplication.run(KafkaProducerTransactionApplication.class, args);
    }
}
