package io.github.loicgreffier.producer.avro.generic;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaProducerAvroGenericApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaProducerAvroGenericApplication.class, args);
    }
}
