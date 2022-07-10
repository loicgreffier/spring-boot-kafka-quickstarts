package io.lgr.quickstarts.streams.map.properties;

import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Getter
@Configuration
@ConfigurationProperties(prefix = "kafka.properties")
public class StreamsProperties {
    private final Map<String, String> streams = new HashMap<>();
    private final Map<String, String> serdes = new HashMap<>();

    public Properties streamsAsProperties() {
        final Properties streamProperties = new Properties();
        streamProperties.putAll(streams);

        return streamProperties;
    }
}
