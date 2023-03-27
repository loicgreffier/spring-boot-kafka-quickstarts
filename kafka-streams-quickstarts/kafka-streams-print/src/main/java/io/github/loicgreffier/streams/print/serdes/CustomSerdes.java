package io.github.loicgreffier.streams.print.serdes;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.Setter;
import org.apache.avro.specific.SpecificRecord;

import java.util.Map;

public class CustomSerdes {
    private CustomSerdes() { }

    @Setter
    private static Map<String, String> serdesConfig;

    public static <T extends SpecificRecord> SpecificAvroSerde<T> getKeySerdes() {
        return getSerdes(true);
    }

    public static <T extends SpecificRecord> SpecificAvroSerde<T> getValueSerdes() {
        return getSerdes(false);
    }

    private static <T extends SpecificRecord> SpecificAvroSerde<T> getSerdes(boolean isSerdeForRecordKeys) {
        SpecificAvroSerde<T> serdes = new SpecificAvroSerde<>();
        serdes.configure(serdesConfig, isSerdeForRecordKeys);
        return serdes;
    }
}
