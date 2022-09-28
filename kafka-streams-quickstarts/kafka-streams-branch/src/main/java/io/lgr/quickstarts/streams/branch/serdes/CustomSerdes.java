package io.lgr.quickstarts.streams.branch.serdes;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.Setter;
import org.apache.avro.specific.SpecificRecord;

import java.util.Map;

public class CustomSerdes {
    private CustomSerdes() { }

    @Setter
    private static Map<String, String> serdesConfig;

    public static <T extends SpecificRecord> SpecificAvroSerde<T> getSerdes() {
        SpecificAvroSerde<T> serdes = new SpecificAvroSerde<>();
        serdes.configure(serdesConfig, false);
        return serdes;
    }
}
