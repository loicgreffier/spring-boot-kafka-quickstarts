package io.lgr.quickstarts.streams.map.app;

import io.lgr.quickstarts.streams.map.properties.StreamsProperties;
import io.lgr.quickstarts.streams.map.serdes.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;

@Slf4j
@Component
public class KafkaStreamsMapRunner implements ApplicationRunner {
    @Autowired
    private ConfigurableApplicationContext applicationContext;

    @Autowired
    private StreamsProperties streamsProperties;

    private KafkaStreams kafkaStreams;

    @Override
    public void run(ApplicationArguments args) {
        CustomSerdes.setSerdesConfig(streamsProperties.getSerdes());

        Topology topology = KafkaStreamsMapTopology.topology();
        log.info("Description of the topology:\n {}", topology.describe());

        kafkaStreams = new KafkaStreams(topology, streamsProperties.streamsAsProperties());

        kafkaStreams.setUncaughtExceptionHandler(exception -> {
            log.error("A not covered exception occurred in {} Kafka Streams. Shutting down...",
                    streamsProperties.streamsAsProperties().get(StreamsConfig.APPLICATION_ID_CONFIG), exception);

            applicationContext.close();
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        kafkaStreams.setStateListener((newState, oldState) -> {
            if (newState.equals(KafkaStreams.State.ERROR)) {
                log.error("The {} Kafka Streams is in error state...",
                        streamsProperties.streamsAsProperties().get(StreamsConfig.APPLICATION_ID_CONFIG));

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
