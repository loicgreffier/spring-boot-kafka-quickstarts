package io.github.loicgreffier.producer.simple.app;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.github.loicgreffier.producer.simple.constants.Topic.STRING_TOPIC;

@Slf4j
@Component
public class ProducerRunner {
    @Autowired
    private Producer<String, String> producer;

    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        int i = 0;
        while (true) {
            ProducerRecord<String, String> message = new ProducerRecord<>(STRING_TOPIC, String.valueOf(i), String.format("Message %s", i));
            message.headers().add("headerKey", "headerValue".getBytes(StandardCharsets.UTF_8));

            send(message);

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                log.error("Interruption during sleep between message production", e);
                Thread.currentThread().interrupt();
            }

            i++;
        }
    }

    public Future<RecordMetadata> send(ProducerRecord<String, String> message) {
        return producer.send(message, ((recordMetadata, e) -> {
            if (e != null) {
                log.error(e.getMessage());
            } else {
                log.info("Success: topic = {}, partition = {}, offset = {}, key = {}, value = {}", recordMetadata.topic(),
                        recordMetadata.partition(), recordMetadata.offset(), message.key(), message.value());
            }
        }));
    }
}
