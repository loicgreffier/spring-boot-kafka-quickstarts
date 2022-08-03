package io.lgr.quickstarts.producer.transactional;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaProducerTransactionalApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaProducerTransactionalApplication.class, args);
    }
}
