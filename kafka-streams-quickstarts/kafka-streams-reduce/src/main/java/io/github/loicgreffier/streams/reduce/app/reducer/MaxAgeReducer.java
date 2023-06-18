package io.github.loicgreffier.streams.reduce.app.reducer;

import io.github.loicgreffier.avro.KafkaPerson;
import org.apache.kafka.streams.kstream.Reducer;

import java.time.LocalDate;
import java.time.ZoneOffset;

public class MaxAgeReducer implements Reducer<KafkaPerson> {
    @Override
    public KafkaPerson apply(KafkaPerson reduced, KafkaPerson toReduce) {
        LocalDate reducedBirthDate = LocalDate.ofInstant(reduced.getBirthDate(), ZoneOffset.UTC);
        LocalDate toReduceBirthDate = LocalDate.ofInstant(toReduce.getBirthDate(), ZoneOffset.UTC);
        return toReduceBirthDate.isBefore(reducedBirthDate) ? toReduce : reduced;
    }
}
