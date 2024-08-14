package io.github.loicgreffier.streams.deserialization.exception.handler.app.handler;

import static io.github.loicgreffier.streams.deserialization.exception.handler.constant.Topic.PERSON_DLQ_TOPIC;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * Custom DeserializationExceptionHandler that sends the record to a dead letter queue (DLQ) topic.
 */
@Slf4j
public class SendToDlqDeserializationExceptionHandler implements DeserializationExceptionHandler {
    private Producer<byte[], byte[]> producer;

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context,
                                                 ConsumerRecord<byte[], byte[]> record,
                                                 Exception exception) {
        log.warn("Exception caught during Deserialization, taskId: {}, topic: {}, partition: {}, offset: {}",
            context.taskId(), record.topic(), record.partition(), record.offset(), exception);

        ProducerRecord<byte[], byte[]> message = new ProducerRecord<>(PERSON_DLQ_TOPIC, record.key(), record.value());
        producer.send(message, ((recordMetadata, e) -> {
            if (e != null) {
                log.error(e.getMessage());
            } else {
                log.info("Success sending to DLQ: topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                    recordMetadata.topic(),
                    recordMetadata.partition(),
                    recordMetadata.offset(), message.key(),
                    message.value());
            }
        }));

        return DeserializationHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Properties producerProperties = new Properties();
        producerProperties.put(BOOTSTRAP_SERVERS_CONFIG, configs.get(BOOTSTRAP_SERVERS_CONFIG));
        producerProperties.put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProperties.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProperties.put(CLIENT_ID_CONFIG, "deserialization-exception-handler-producer");
        producer = new KafkaProducer<>(producerProperties);
    }
}
