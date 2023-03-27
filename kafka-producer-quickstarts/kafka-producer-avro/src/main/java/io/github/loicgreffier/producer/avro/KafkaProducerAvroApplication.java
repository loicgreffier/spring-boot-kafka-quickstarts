package io.github.loicgreffier.producer.avro;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaProducerAvroApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaProducerAvroApplication.class, args);
    }
}
