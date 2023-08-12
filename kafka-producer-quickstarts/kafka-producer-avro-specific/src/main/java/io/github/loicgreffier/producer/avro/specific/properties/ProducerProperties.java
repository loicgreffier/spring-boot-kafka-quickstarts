package io.github.loicgreffier.producer.avro.specific.properties;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * This class represents Kafka producer properties configuration.
 */
@Getter
@Configuration
@ConfigurationProperties(prefix = "kafka")
public class ProducerProperties {
    private final Map<String, String> properties = new HashMap<>();

    /**
     * Converts the producer properties into a Java Properties object.
     *
     * @return A Properties object containing the Kafka producer properties.
     */
    public Properties asProperties() {
        Properties config = new Properties();
        config.putAll(properties);
        return config;
    }
}
