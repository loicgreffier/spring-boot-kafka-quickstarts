package io.github.loicgreffier.streams.producer.country.app;

import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaCountry;
import jakarta.annotation.PreDestroy;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.Future;

import static io.github.loicgreffier.streams.producer.country.constants.Topic.COUNTRY_TOPIC;

@Slf4j
@Component
@AllArgsConstructor
public class KafkaStreamsProducerCountryRunner implements ApplicationRunner {
    private final Producer<String, KafkaCountry> kafkaProducer;

    @Override
    public void run(ApplicationArguments args) {
        for (KafkaCountry country : buildKafkaCountries()) {
            ProducerRecord<String, KafkaCountry> message = new ProducerRecord<>(COUNTRY_TOPIC, country.getCode().toString(), country);

            send(message);
        }
    }

    public Future<RecordMetadata> send(ProducerRecord<String, KafkaCountry> message) {
        return kafkaProducer.send(message, ((recordMetadata, e) -> {
            if (e != null) {
                log.error(e.getMessage());
            } else {
                log.info("Success: topic = {}, partition = {}, offset = {}, key = {}, value = {}", recordMetadata.topic(),
                        recordMetadata.partition(), recordMetadata.offset(), message.key(), message.value());
            }
        }));
    }

    private List<KafkaCountry> buildKafkaCountries() {
        KafkaCountry france = KafkaCountry.newBuilder()
                .setCode(CountryCode.FR)
                .setName("France")
                .setCapital("Paris")
                .setOfficialLanguage("French")
                .build();

        KafkaCountry germany = KafkaCountry.newBuilder()
                .setCode(CountryCode.DE)
                .setName("Germany")
                .setCapital("Berlin")
                .setOfficialLanguage("German")
                .build();

        KafkaCountry spain = KafkaCountry.newBuilder()
                .setCode(CountryCode.ES)
                .setName("Spain")
                .setCapital("Madrid")
                .setOfficialLanguage("Spanish")
                .build();

        KafkaCountry italy = KafkaCountry.newBuilder()
                .setCode(CountryCode.IT)
                .setName("Italy")
                .setCapital("Rome")
                .setOfficialLanguage("Italian")
                .build();

        KafkaCountry unitedKingdom = KafkaCountry.newBuilder()
                .setCode(CountryCode.GB)
                .setName("United Kingdom")
                .setCapital("London")
                .setOfficialLanguage("English")
                .build();

        return List.of(france, germany, spain, italy, unitedKingdom);
    }

    @PreDestroy
    public void preDestroy() {
        log.info("Closing producer");
        kafkaProducer.close();
    }
}
