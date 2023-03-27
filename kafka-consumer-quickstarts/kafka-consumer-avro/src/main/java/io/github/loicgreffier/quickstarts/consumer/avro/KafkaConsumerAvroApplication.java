package io.github.loicgreffier.quickstarts.consumer.avro;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaConsumerAvroApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerAvroApplication.class, args);
    }
}
