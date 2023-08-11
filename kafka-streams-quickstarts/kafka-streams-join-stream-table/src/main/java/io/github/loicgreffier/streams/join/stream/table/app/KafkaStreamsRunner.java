package io.github.loicgreffier.streams.join.stream.table.app;

import io.github.loicgreffier.streams.join.stream.table.properties.KafkaStreamsProperties;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaStreamsRunner {
    @Autowired
    private ConfigurableApplicationContext applicationContext;

    @Autowired
    private KafkaStreamsProperties properties;

    private KafkaStreams kafkaStreams;

    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsTopology.topology(streamsBuilder);
        Topology topology = streamsBuilder.build();
        log.info("Topology description:\n {}", topology.describe());

        kafkaStreams = new KafkaStreams(topology, properties.asProperties());

        kafkaStreams.setUncaughtExceptionHandler(exception -> {
            log.error("A not covered exception occurred in {} Kafka Streams. Shutting down...",
                    properties.asProperties().get(StreamsConfig.APPLICATION_ID_CONFIG), exception);

            applicationContext.close();
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        kafkaStreams.setStateListener((newState, oldState) -> {
            if (newState.equals(KafkaStreams.State.ERROR)) {
                log.error("The {} Kafka Streams is in error state...",
                        properties.asProperties().get(StreamsConfig.APPLICATION_ID_CONFIG));

                applicationContext.close();
            }
        });

        kafkaStreams.start();
    }

    @PreDestroy
    public void preDestroy() {
        log.info("Closing streams");
        kafkaStreams.close();
    }
}
