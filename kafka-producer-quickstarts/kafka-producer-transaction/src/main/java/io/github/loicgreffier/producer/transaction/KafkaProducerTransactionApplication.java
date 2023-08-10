package io.github.loicgreffier.producer.transaction;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@SpringBootApplication
public class KafkaProducerTransactionApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaProducerTransactionApplication.class, args);
    }
}
