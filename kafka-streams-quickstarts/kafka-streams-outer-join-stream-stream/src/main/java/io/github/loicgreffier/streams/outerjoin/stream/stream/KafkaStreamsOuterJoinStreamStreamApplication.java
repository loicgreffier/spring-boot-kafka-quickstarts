package io.github.loicgreffier.streams.outerjoin.stream.stream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * This is the main class for the Kafka Streams application.
 */
@SpringBootApplication
public class KafkaStreamsOuterJoinStreamStreamApplication {
    /**
     * The main entry point of the Kafka Streams application.
     *
     * @param args The command line arguments.
     */
    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsOuterJoinStreamStreamApplication.class, args);
    }
}
