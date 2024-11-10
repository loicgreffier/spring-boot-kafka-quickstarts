package io.github.loicgreffier.streams.leftjoin.stream.table.serdes;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.avro.specific.SpecificRecord;

/**
 * Utility class for Serdes.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SerdesUtils {
    @Setter
    private static Map<String, String> serdesConfig;

    /**
     * Create a SpecificAvroSerde for the value.
     *
     * @param <T> the type of the value.
     * @return the SpecificAvroSerde.
     */
    public static <T extends SpecificRecord> SpecificAvroSerde<T> getValueSerdes() {
        SpecificAvroSerde<T> serdes = new SpecificAvroSerde<>();
        serdes.configure(serdesConfig, false);
        return serdes;
    }
}
