package io.github.loicgreffier.consumer.avro.specific;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@SpringBootApplication
public class KafkaConsumerAvroSpecificApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerAvroSpecificApplication.class, args);
    }
}
