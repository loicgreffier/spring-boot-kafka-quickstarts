package io.github.loicgreffier.streams.aggregate.tumbling.window;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaStreamsAggregateTumblingWindowApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsAggregateTumblingWindowApplication.class, args);
    }
}
