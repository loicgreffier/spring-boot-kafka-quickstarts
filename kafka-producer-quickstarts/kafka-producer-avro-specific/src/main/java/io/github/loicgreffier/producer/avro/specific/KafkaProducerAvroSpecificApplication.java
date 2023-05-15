package io.github.loicgreffier.producer.avro.specific;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaProducerAvroSpecificApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaProducerAvroSpecificApplication.class, args);
    }
}
