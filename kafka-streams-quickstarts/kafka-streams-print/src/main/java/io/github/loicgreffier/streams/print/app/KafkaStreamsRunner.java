package io.github.loicgreffier.streams.print.app;

import io.github.loicgreffier.streams.print.property.ApplicationProperties;
import io.github.loicgreffier.streams.print.property.KafkaStreamsProperties;
import io.github.loicgreffier.streams.print.serdes.SerdesUtils;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * This class represents a Kafka Streams runner that runs a topology.
 */
@Slf4j
@Component
public class KafkaStreamsRunner {
    @Autowired
    private ConfigurableApplicationContext applicationContext;

    @Autowired
    private KafkaStreamsProperties kafkaStreamsProperties;

    @Autowired
    private ApplicationProperties applicationProperties;

    private KafkaStreams kafkaStreams;

    /**
     * Starts the Kafka Streams when the application is ready.
     * The Kafka Streams topology is built in the {@link KafkaStreamsTopology} class.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void run() throws IOException {
        SerdesUtils.setSerdesConfig(kafkaStreamsProperties.getProperties());

        Path filePath = Paths.get(applicationProperties.getFilePath()
            .substring(0, applicationProperties.getFilePath().lastIndexOf("/")));
        if (!Files.exists(filePath)) {
            Files.createDirectories(filePath);
        }

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsTopology.topology(streamsBuilder, applicationProperties.getFilePath());
        Topology topology = streamsBuilder.build();
        log.info("Topology description:\n {}", topology.describe());

        kafkaStreams = new KafkaStreams(topology, kafkaStreamsProperties.asProperties());

        kafkaStreams.setUncaughtExceptionHandler(exception -> {
            log.error("A not covered exception occurred in {} Kafka Streams. Shutting down...",
                kafkaStreamsProperties.asProperties().get(StreamsConfig.APPLICATION_ID_CONFIG),
                exception);

            applicationContext.close();
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        kafkaStreams.setStateListener((newState, oldState) -> {
            if (newState.equals(KafkaStreams.State.ERROR)) {
                log.error("The {} Kafka Streams is in error state...",
                    kafkaStreamsProperties.asProperties().get(StreamsConfig.APPLICATION_ID_CONFIG));

                applicationContext.close();
            }
        });

        kafkaStreams.start();
    }

    /**
     * Closes the Kafka Streams when the application is stopped.
     */
    @PreDestroy
    public void preDestroy() {
        log.info("Closing streams");
        kafkaStreams.close();
    }
}
