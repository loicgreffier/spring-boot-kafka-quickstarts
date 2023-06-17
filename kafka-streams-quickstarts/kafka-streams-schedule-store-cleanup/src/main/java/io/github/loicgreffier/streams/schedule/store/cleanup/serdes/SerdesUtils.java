package io.github.loicgreffier.streams.schedule.store.cleanup.serdes;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.avro.specific.SpecificRecord;

import java.util.Map;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SerdesUtils {
    @Setter
    private static Map<String, String> serdesConfig;

    public static <T extends SpecificRecord> SpecificAvroSerde<T> specificAvroValueSerdes() {
        SpecificAvroSerde<T> serdes = new SpecificAvroSerde<>();
        serdes.configure(serdesConfig, false);
        return serdes;
    }
}
