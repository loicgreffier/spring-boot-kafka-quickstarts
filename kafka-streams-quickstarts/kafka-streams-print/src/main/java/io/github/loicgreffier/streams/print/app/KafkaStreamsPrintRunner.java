package io.github.loicgreffier.streams.print.app;

import io.github.loicgreffier.streams.print.properties.ApplicationProperties;
import io.github.loicgreffier.streams.print.properties.KafkaStreamsProperties;
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
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
@Component
public class KafkaStreamsPrintRunner implements ApplicationRunner {
    @Autowired
    private ConfigurableApplicationContext applicationContext;

    @Autowired
    private KafkaStreamsProperties kafkaStreamsProperties;

    @Autowired
    private ApplicationProperties applicationProperties;

    private KafkaStreams kafkaStreams;

    @Override
    public void run(ApplicationArguments args) throws IOException {
        Path filePath = Paths.get(applicationProperties.getFilePath().substring(0, applicationProperties.getFilePath().lastIndexOf("/")));
        if (!Files.exists(filePath)) {
            Files.createDirectories(filePath);
        }

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsPrintTopology.topology(streamsBuilder, applicationProperties.getFilePath());
        Topology topology = streamsBuilder.build();
        log.info("Topology description:\n {}", topology.describe());

        kafkaStreams = new KafkaStreams(topology, kafkaStreamsProperties.asProperties());

        kafkaStreams.setUncaughtExceptionHandler(exception -> {
            log.error("A not covered exception occurred in {} Kafka Streams. Shutting down...",
                    kafkaStreamsProperties.asProperties().get(StreamsConfig.APPLICATION_ID_CONFIG), exception);

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

    @PreDestroy
    public void preDestroy() {
        log.info("Closing streams");
        kafkaStreams.close();
    }
}
