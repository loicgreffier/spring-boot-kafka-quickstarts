package io.lgr.quickstarts.producer.simple.app;

import io.lgr.quickstarts.producer.simple.constants.Topic;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Component
@AllArgsConstructor
public class KafkaProducerSimpleRunner implements ApplicationRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerSimpleRunner.class);

    @Autowired
    private Producer<String, String> kafkaProducer;

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
                LOGGER.error("Interruption during sleep between message production", e);
                Thread.currentThread().interrupt();
            }

            i++;
        }
    }

    public Future<RecordMetadata> send(ProducerRecord<String, String> message) {
        return kafkaProducer.send(message, ((recordMetadata, e) -> {
            if (e != null) {
                LOGGER.error(e.getMessage());
            } else {
                LOGGER.info("Success: topic = {} partition = {} offset = {}, key = {}, value = {}", recordMetadata.topic(),
                        recordMetadata.partition(), recordMetadata.offset(), message.key(), message.value());
            }
        }));
    }

    @PreDestroy
    public void preDestroy() {
        kafkaProducer.close();
    }
}
