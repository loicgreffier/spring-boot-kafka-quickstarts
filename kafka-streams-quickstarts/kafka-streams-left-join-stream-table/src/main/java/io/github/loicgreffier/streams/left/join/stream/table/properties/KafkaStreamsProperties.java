package io.github.loicgreffier.streams.left.join.stream.table.properties;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * This class represents Kafka Streams properties configuration.
 */
@Getter
@Configuration
@ConfigurationProperties(prefix = "kafka")
public class KafkaStreamsProperties {
    private final Map<String, String> properties = new HashMap<>();

    /**
     * Converts the Kafka Streams properties into a Java Properties object.
     *
     * @return A Properties object containing the Kafka Streams properties.
     */
    public Properties asProperties() {
        Properties config = new Properties();
        config.putAll(properties);
        return config;
    }
}
