package io.lgr.quickstarts.streams.map.properties;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

@Getter
@Configuration
@ConfigurationProperties(prefix = "kafka")
public class StreamsProperties {
    private final Map<String, String> properties = new HashMap<>();

    public Properties asProperties() {
        final Properties streamProperties = new Properties();
        streamProperties.putAll(this.properties);

        return streamProperties;
    }

    public Map<String, String> getSerdesProperties() {
        return properties.entrySet()
                .stream()
                .filter(entry -> KafkaAvroSerializerConfig.baseConfigDef().configKeys().containsKey(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
