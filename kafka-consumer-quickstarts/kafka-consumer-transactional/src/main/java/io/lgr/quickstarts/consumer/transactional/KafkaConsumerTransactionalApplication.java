package io.lgr.quickstarts.consumer.transactional;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaConsumerTransactionalApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerTransactionalApplication.class, args);
    }
}
