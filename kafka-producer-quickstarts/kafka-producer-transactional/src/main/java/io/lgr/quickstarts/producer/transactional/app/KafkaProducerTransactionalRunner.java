package io.lgr.quickstarts.producer.transactional.app;

import io.lgr.quickstarts.producer.transactional.constants.Topic;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@AllArgsConstructor
public class KafkaProducerTransactionalRunner implements ApplicationRunner {
    private final Producer<String, String> kafkaProducer;

    @Override
    public void run(ApplicationArguments args) {
        log.info("Init transactions");
        kafkaProducer.initTransactions();

        int i = 0;
        int max = args.containsOption("iteration") ? Integer.parseInt(args.getOptionValues("iteration").get(0)) : 10;
        while (i < max) {
            try {
                log.info("Begin transaction");
                kafkaProducer.beginTransaction();

                ProducerRecord<String, String> firstMessage = new ProducerRecord<>(Topic.FIRST_STRING_TOPIC.toString(),
                        String.valueOf(i), String.format("Message %s", i));

                send(firstMessage);

                ProducerRecord<String, String> secondMessage = new ProducerRecord<>(Topic.SECOND_STRING_TOPIC.toString(),
                        String.valueOf(i), String.format("Message %s", i));

                send(secondMessage);

                if (i % 3 == 0) {
                    throw new Exception("Error during transaction...");
                }

                log.info("Commit transaction");
                kafkaProducer.commitTransaction();

                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    log.error("Interruption during sleep between message production", e);
                    Thread.currentThread().interrupt();
                }
            } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
                log.info("Closing producer");
                kafkaProducer.close();
            } catch (Exception e) {
                log.info("Abort transaction", e);
                kafkaProducer.abortTransaction();
            }

            i++;
        }
    }

    public Future<RecordMetadata> send(ProducerRecord<String, String> message) {
        return kafkaProducer.send(message, ((recordMetadata, e) -> {
            if (e != null) {
                log.info("Fail: topic = {} partition = {} offset = {}, key = {}, value = {}", recordMetadata.topic(),
                        recordMetadata.partition(), recordMetadata.offset(), message.key(), message.value());
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
