package io.github.loicgreffier.producer.avro.specific;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@SpringBootApplication
public class KafkaProducerAvroSpecificApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaProducerAvroSpecificApplication.class, args);
    }
}
