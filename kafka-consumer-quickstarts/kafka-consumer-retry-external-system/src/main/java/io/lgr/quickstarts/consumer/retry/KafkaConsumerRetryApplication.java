package io.lgr.quickstarts.consumer.retry;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaConsumerRetryApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerRetryApplication.class, args);
    }
}
