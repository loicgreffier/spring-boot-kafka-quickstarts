package io.lgr.quickstarts.consumer.retry.services;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@Service
public class ExternalService {
    private final Logger LOGGER = LoggerFactory.getLogger(ExternalService.class);
    private final Random random = new Random();

    @Value("${failureRate}")
    private int failureRate;

    public void call(ConsumerRecord<String, String> message) throws Exception {
        int duration = random.nextInt(1000);

        LOGGER.info("Simulating a call to an external system that will take {} for message {}", duration, message.offset());

        TimeUnit.MILLISECONDS.sleep(duration);
        if (random.nextInt(100) < failureRate) {
            throw new Exception("Call to external system failed");
        }
    }
}
