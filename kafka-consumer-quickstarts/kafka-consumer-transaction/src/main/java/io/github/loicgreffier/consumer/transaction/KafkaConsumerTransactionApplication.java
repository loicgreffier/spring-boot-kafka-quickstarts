package io.github.loicgreffier.consumer.transaction;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@SpringBootApplication
public class KafkaConsumerTransactionApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerTransactionApplication.class, args);
    }
}
