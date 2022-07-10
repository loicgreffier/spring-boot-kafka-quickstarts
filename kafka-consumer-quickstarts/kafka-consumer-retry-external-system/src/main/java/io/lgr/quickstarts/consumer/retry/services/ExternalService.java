package io.lgr.quickstarts.consumer.retry.services;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class ExternalService {
    private final Random random = new Random();

    @Value("${failureRate}")
    private int failureRate;

    public void call(ConsumerRecord<String, String> message) throws Exception {
        int duration = random.nextInt(1000);

        log.info("Simulating a call to an external system that will take {} for message {}", duration, message.offset());

        TimeUnit.MILLISECONDS.sleep(duration);
        if (random.nextInt(100) < failureRate) {
            throw new Exception("Call to external system failed");
        }
    }
}
