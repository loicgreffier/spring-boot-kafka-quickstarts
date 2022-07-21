package io.lgr.quickstarts.producer.simple.app;

import io.lgr.quickstarts.producer.simple.constants.Topic;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@AllArgsConstructor
public class KafkaProducerSimpleRunner implements ApplicationRunner {
    private final Producer<String, String> kafkaProducer;

    @Override
    public void run(ApplicationArguments args) {
        int i = 0;
        while (true) {
            ProducerRecord<String, String> message = new ProducerRecord<>(Topic.STRING_TOPIC.toString(),
                    String.valueOf(i), String.format("Message %s", i));
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
        return kafkaProducer.send(message, ((recordMetadata, e) -> {
            if (e != null) {
                log.error(e.getMessage());
            } else {
                log.info("Success: topic = {} partition = {} offset = {}, key = {}, value = {}", recordMetadata.topic(),
                        recordMetadata.partition(), recordMetadata.offset(), message.key(), message.value());
            }
        }));
    }

    @PreDestroy
    public void preDestroy() {
        log.info("Closing producer");
        kafkaProducer.close();
    }
}
