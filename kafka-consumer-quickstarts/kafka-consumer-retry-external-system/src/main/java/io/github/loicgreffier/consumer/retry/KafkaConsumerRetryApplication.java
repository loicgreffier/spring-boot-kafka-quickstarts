package io.github.loicgreffier.consumer.retry;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@SpringBootApplication
public class KafkaConsumerRetryApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerRetryApplication.class, args);
    }
}
