package io.github.loicgreffier.producer.headers;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

/**
 * This is the main class for the Kafka producer application.
 */
@EnableAsync
@SpringBootApplication
public class KafkaProducerHeadersApplication {
    /**
     * The main entry point of the Kafka producer application.
     *
     * @param args The command line arguments.
     */
    public static void main(String[] args) {
        SpringApplication.run(KafkaProducerHeadersApplication.class, args);
    }
}
