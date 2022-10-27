package io.lgr.quickstarts.streams.count;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaStreamsCountApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsCountApplication.class, args);
    }
}
