package io.lgr.quickstarts.consumer.circuit.breaker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaConsumerCircuitBreakerApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerCircuitBreakerApplication.class, args);
    }
}
