package io.github.loicgreffier.consumer.circuit.breaker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@SpringBootApplication
public class KafkaConsumerCircuitBreakerApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerCircuitBreakerApplication.class, args);
    }
}
