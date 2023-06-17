package io.github.loicgreffier.streams.schedule.properties;

import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Getter
@Configuration
@ConfigurationProperties(prefix = "kafka")
public class KafkaStreamsProperties {
    private final Map<String, String> properties = new HashMap<>();

    public Properties asProperties() {
        final Properties config = new Properties();
        config.putAll(properties);
        return config;
    }
}
