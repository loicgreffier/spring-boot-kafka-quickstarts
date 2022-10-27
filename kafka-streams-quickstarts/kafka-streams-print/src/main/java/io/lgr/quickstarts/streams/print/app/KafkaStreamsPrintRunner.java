package io.lgr.quickstarts.streams.print.app;

import io.lgr.quickstarts.streams.print.properties.ApplicationProperties;
import io.lgr.quickstarts.streams.print.properties.StreamsProperties;
import io.lgr.quickstarts.streams.print.serdes.CustomSerdes;
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

import javax.annotation.PreDestroy;
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
    private StreamsProperties streamsProperties;

    @Autowired
    private ApplicationProperties applicationProperties;

    private KafkaStreams kafkaStreams;

    @Override
    public void run(ApplicationArguments args) throws IOException {
        CustomSerdes.setSerdesConfig(streamsProperties.getProperties());

        Path filePath = Paths.get(applicationProperties.getFilePath().substring(0, applicationProperties.getFilePath().lastIndexOf("/")));
        if (!Files.exists(filePath)) {
            Files.createDirectories(filePath);
        }

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsPrintTopology.topology(streamsBuilder, applicationProperties.getFilePath());
        Topology topology = streamsBuilder.build();
        log.info("Description of the topology:\n {}", topology.describe());

        kafkaStreams = new KafkaStreams(topology, streamsProperties.asProperties());

        kafkaStreams.setUncaughtExceptionHandler(exception -> {
            log.error("A not covered exception occurred in {} Kafka Streams. Shutting down...",
                    streamsProperties.asProperties().get(StreamsConfig.APPLICATION_ID_CONFIG), exception);

            applicationContext.close();
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        kafkaStreams.setStateListener((newState, oldState) -> {
            if (newState.equals(KafkaStreams.State.ERROR)) {
                log.error("The {} Kafka Streams is in error state...",
                        streamsProperties.asProperties().get(StreamsConfig.APPLICATION_ID_CONFIG));

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
