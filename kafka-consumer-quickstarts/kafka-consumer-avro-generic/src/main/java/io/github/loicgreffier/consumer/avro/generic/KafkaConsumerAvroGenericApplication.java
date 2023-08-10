package io.github.loicgreffier.consumer.avro.generic;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@SpringBootApplication
public class KafkaConsumerAvroGenericApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerAvroGenericApplication.class, args);
    }
}
