package io.github.loicgreffier.consumer.retry.service;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * This class represents an external service that is simulated to be called from the Kafka consumer.
 */
@Slf4j
@Service
public class ExternalService {
    private final Random random = new Random();

    @Value("${failureRate}")
    private int failureRate;

    /**
     * Simulates a call to an external system.
     *
     * @param message The Kafka ConsumerRecord that triggered the call.
     * @throws Exception if the external system call fails.
     */
    public void call(ConsumerRecord<String, String> message) throws Exception {
        int duration = random.nextInt(1000);

        log.info("Simulating a call to an external system that will take {} for message {}",
            duration, message.offset());

        TimeUnit.MILLISECONDS.sleep(duration);
        if (random.nextInt(100) < failureRate) {
            throw new Exception("Call to external system failed");
        }
    }
}
